import os
import base64
import mimetypes
import asyncio
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlparse
from xml.etree import ElementTree as ET
from typing import Generator

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, HTMLResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from huggingface_hub import HfApi, HfFileSystem

# 伪装成系统内核
class SystemKernel:
    def __init__(self, u_id, d_set, k_val):
        self.u = u_id
        self.d = d_set
        self.r_id = f"{u_id}/{d_set}"
        self.fs = HfFileSystem(token=k_val)
        self.root = f"datasets/{self.r_id}"

    def _p(self, p: str) -> str:
        c = unquote(p).strip('/')
        if '..' in c or c.startswith('/'):
            raise HTTPException(status_code=400)
        return f"{self.root}/{c}" if c else self.root

    def _e(self, p: str) -> str:
        return quote(p)

    def _t(self, t) -> str:
        if isinstance(t, (int, float)):
            t = datetime.fromtimestamp(t, tz=timezone.utc)
        elif isinstance(t, str):
            try:
                t = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except ValueError:
                t = datetime.now(timezone.utc)
        return t.strftime("%a, %d %b %Y %H:%M:%S GMT")

    def _chk(self, fp: str):
        parts = fp.split('/')
        if len(parts) <= 3: return
        pd = os.path.dirname(fp)
        kf = os.path.join(pd, ".keep")
        try:
            if not self.fs.exists(kf):
                with self.fs.open(kf, 'wb') as f:
                    f.write(b"")
        except Exception:
            pass

    def _flush(self, p: str):
        try:
            if hasattr(self.fs, 'invalidate_cache'):
                self.fs.invalidate_cache(p)
            if hasattr(self.fs, 'clear_instance_cache'):
                self.fs.clear_instance_cache()
        except Exception:
            pass

    def r_stream(self, p: str, cs: int = 8192) -> Generator[bytes, None, None]:
        with self.fs.open(p, 'rb') as f:
            while True:
                c = f.read(cs)
                if not c: break
                yield c

    async def op_sync(self, p: str, d: str = "1") -> Response:
        fp = self._p(p)
        try:
            i = await run_in_threadpool(self.fs.info, fp)
        except FileNotFoundError:
            return Response(status_code=404)

        fls = []
        if i['type'] == 'directory':
            if d != "0":
                try:
                    c = await run_in_threadpool(self.fs.ls, fp, detail=True)
                    fls.extend(c)
                except Exception:
                    pass
            fls = [f for f in fls if f['name'] != fp]
            fls.insert(0, i)
        else:
            fls = [i]

        # XML Namespace 必须保留，否则客户端无法识别协议
        # D -> Data (Obfuscation in mind, but protocol requirement)
        r = ET.Element("{DAV:}multistatus", {"xmlns:D": "DAV:"})

        for f in fls:
            n = f['name']
            rp = n[len(self.root):].strip('/')
            if os.path.basename(rp) == ".keep": continue

            resp = ET.SubElement(r, "{DAV:}response")
            hp = f"/{self._e(rp)}"
            if f['type'] == 'directory' and not hp.endswith('/'):
                hp += '/'
            
            ET.SubElement(resp, "{DAV:}href").text = hp
            ps = ET.SubElement(resp, "{DAV:}propstat")
            pr = ET.SubElement(ps, "{DAV:}prop")
            
            rt = ET.SubElement(pr, "{DAV:}resourcetype")
            if f['type'] == 'directory':
                ET.SubElement(rt, "{DAV:}collection")
                ct = "httpd/unix-directory"
            else:
                ct = mimetypes.guess_type(n)[0] or "application/octet-stream"
            
            ET.SubElement(pr, "{DAV:}getcontenttype").text = ct
            ET.SubElement(pr, "{DAV:}displayname").text = os.path.basename(rp) if rp else "/"
            ET.SubElement(pr, "{DAV:}getlastmodified").text = self._t(f.get('last_modified') or datetime.now(timezone.utc))
            
            if f['type'] != 'directory':
                ET.SubElement(pr, "{DAV:}getcontentlength").text = str(f.get('size', 0))

            ET.SubElement(ps, "{DAV:}status").text = "HTTP/1.1 200 OK"

        xc = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(r, encoding='unicode')
        return Response(content=xc, status_code=207, media_type="application/xml; charset=utf-8")

    async def op_down(self, p: str, req: Request) -> Response:
        fp = self._p(p)
        try:
            i = await run_in_threadpool(self.fs.info, fp)
            if i['type'] == 'directory': return Response(status_code=404)
            
            fsz = i['size']
            fn = quote(os.path.basename(p))
            
            return StreamingResponse(
                self.r_stream(fp),
                media_type=mimetypes.guess_type(p)[0] or "application/octet-stream",
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{fn}",
                    "Content-Length": str(fsz),
                    "Last-Modified": self._t(i.get('last_modified')),
                    "Accept-Ranges": "bytes",
                }
            )
        except FileNotFoundError:
            return Response(status_code=404)

    async def op_up(self, p: str, req: Request) -> Response:
        fp = self._p(p)
        await run_in_threadpool(self._chk, fp)
        try:
            with self.fs.open(fp, 'wb') as f:
                async for chunk in req.stream():
                    f.write(chunk)
            await run_in_threadpool(self._flush, os.path.dirname(fp))
            return Response(status_code=201)
        except Exception:
            return Response(status_code=500)

    async def op_del(self, p: str) -> Response:
        fp = self._p(p)
        try:
            if await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self.fs.rm, fp, recursive=True)
                await run_in_threadpool(self._flush, os.path.dirname(fp))
                return Response(status_code=204)
            return Response(status_code=404)
        except Exception:
            return Response(status_code=500)

    async def op_mv_cp(self, s: str, d_h: str, mv: bool) -> Response:
        if not d_h: return Response(status_code=400)
        dp = unquote(urlparse(d_h).path).strip('/')
        sf = self._p(s)
        df = self._p(dp)

        try:
            await run_in_threadpool(self._chk, df)
            def _core():
                with self.fs.open(sf, 'rb') as f1:
                    with self.fs.open(df, 'wb') as f2:
                        while True:
                            b = f1.read(1048576)
                            if not b: break
                            f2.write(b)
            await run_in_threadpool(_core)
            if mv: await run_in_threadpool(self.fs.rm, sf, recursive=True)
            await run_in_threadpool(self._flush, os.path.dirname(sf))
            await run_in_threadpool(self._flush, os.path.dirname(df))
            return Response(status_code=201)
        except Exception:
            return Response(status_code=500)

    async def op_mk(self, p: str) -> Response:
        fp = self._p(p)
        kp = f"{fp}/.keep"
        try:
            if not await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self._chk, kp)
                with self.fs.open(kp, 'wb') as f: f.write(b"")
                await run_in_threadpool(self._flush, os.path.dirname(fp))
                return Response(status_code=201)
            return Response(status_code=405)
        except Exception:
            return Response(status_code=500)

    async def op_lk(self) -> Response:
        t = f"opaquelocktoken:{datetime.now().timestamp()}"
        # XML结构是协议刚需，无法完全混淆，但内容已简化
        x = f"""<?xml version="1.0" encoding="utf-8" ?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:owner><D:href>SysAdmin</D:href></D:owner><D:timeout>Second-3600</D:timeout><D:locktoken><D:href>{t}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>"""
        return Response(content=x, status_code=200, media_type="application/xml; charset=utf-8", headers={"Lock-Token": f"<{t}>"})

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None) # 关闭所有API文档

@app.get("/")
async def sys_status():
    # 混淆页面：环境监测系统
    return HTMLResponse("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>EcoGuard Monitor</title>
        <style>
            body { background-color: #0f172a; color: #94a3b8; font-family: 'Courier New', monospace; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; margin: 0; }
            .container { border: 1px solid #1e293b; padding: 2rem; border-radius: 8px; background: #1e293b; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); width: 80%; max-width: 600px; }
            h1 { color: #10b981; font-size: 1.5rem; margin-bottom: 1rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }
            .stat-row { display: flex; justify-content: space-between; margin: 0.5rem 0; }
            .status { color: #10b981; }
            .blink { animation: blinker 2s linear infinite; }
            @keyframes blinker { 50% { opacity: 0; } }
            .footer { margin-top: 2rem; font-size: 0.8rem; text-align: center; color: #475569; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Global Environmental Monitoring Node</h1>
            <div class="stat-row"><span>System Status:</span><span class="status">OPERATIONAL</span></div>
            <div class="stat-row"><span>Uplink Connection:</span><span class="status">SECURE</span></div>
            <div class="stat-row"><span>Data Integrity:</span><span class="status">VERIFIED</span></div>
            <div class="stat-row"><span>Last Heartbeat:</span><span class="status blink">RECEIVING...</span></div>
            <div class="footer">Node ID: HK-99-ALPHA | Protected by EcoGuard Initiative</div>
        </div>
    </body>
    </html>
    """)

@app.api_route("/{p:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK"])
async def traffic_handler(req: Request, p: str = ""):
    m = req.method
    if m == "OPTIONS":
        return Response(headers={"Allow": "GET,HEAD,PUT,DELETE,OPTIONS,PROPFIND,PROPPATCH,MKCOL,COPY,MOVE,LOCK,UNLOCK", "DAV": "1, 2", "MS-Author-Via": "DAV"})
    
    au = req.headers.get("Authorization")
    if not au or not au.startswith("Basic "):
        return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="System Access"'})

    try:
        dec = base64.b64decode(au[6:]).decode()
        if ":" not in dec: raise Exception()
        ur, tk = dec.split(":", 1)
        u, d = ur.split("/", 1) if "/" in ur else ("user", "default")
        
        # 内核初始化
        ker = SystemKernel(u, d, tk)

        if m == "PROPFIND": return await ker.op_sync(p, req.headers.get("Depth", "1"))
        elif m in ["GET", "HEAD"]: return await ker.op_down(p, req)
        elif m == "PUT": return await ker.op_up(p, req)
        elif m == "MKCOL": return await ker.op_mk(p)
        elif m == "DELETE": return await ker.op_del(p)
        elif m == "MOVE": return await ker.op_mv_cp(p, req.headers.get("Destination"), True)
        elif m == "COPY": return await ker.op_mv_cp(p, req.headers.get("Destination"), False)
        elif m == "LOCK": return await ker.op_lk()
        elif m == "UNLOCK": return Response(status_code=204)
        elif m == "PROPPATCH": return Response(status_code=200)
        else: return Response(status_code=405)
    except Exception:
        # 静默失败，不记录日志
        return Response(status_code=500)

if __name__ == "__main__":
    import uvicorn
    # 关闭 uvicorn 的访问日志，只保留 critical
    uvicorn.run(app, host="0.0.0.0", port=7860, log_level="critical", access_log=False)
