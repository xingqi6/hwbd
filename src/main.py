import os
import base64
import mimetypes
import logging
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlparse
from xml.etree import ElementTree as ET
from typing import Generator, Optional

from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from huggingface_hub import HfFileSystem

# 彻底静默日志，防止泄露
logging.getLogger("uvicorn").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.error").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.access").setLevel(logging.CRITICAL)
logging.getLogger("huggingface_hub").setLevel(logging.CRITICAL)

class SystemKernel:
    def __init__(self, u_id, d_set, k_val):
        self.u = u_id
        self.d = d_set
        self.r_id = f"{u_id}/{d_set}"
        self.fs = HfFileSystem(token=k_val)
        self.root = f"datasets/{self.r_id}"

    def _p(self, p: str) -> str:
        # 路径清洗，防止特殊字符导致路径错误
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

    # 核心修复：支持 Range Seek 的流生成器
    def r_stream(self, p: str, start: int = 0, length: Optional[int] = None, cs: int = 8192) -> Generator[bytes, None, None]:
        try:
            with self.fs.open(p, 'rb') as f:
                if start > 0:
                    f.seek(start)
                
                remaining = length if length is not None else float('inf')
                
                while remaining > 0:
                    read_size = min(cs, remaining) if remaining != float('inf') else cs
                    c = f.read(read_size)
                    if not c:
                        break
                    yield c
                    if remaining != float('inf'):
                        remaining -= len(c)
        except Exception:
            # 发生流错误时静默结束，防止抛出 500 导致连接重置
            pass

    async def op_sync(self, p: str, d: str = "1") -> Response:
        fp = self._p(p)
        try:
            # 必须在线程池运行，防止阻塞
            i = await run_in_threadpool(self.fs.info, fp)
        except FileNotFoundError:
            return Response(status_code=404)
        except Exception:
            return Response(status_code=500)

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
            
            file_size = i['size']
            last_mod = self._t(i.get('last_modified'))
            file_name = quote(os.path.basename(p))
            
            # 处理 Range 头 (AList/浏览器预览关键逻辑)
            range_header = req.headers.get("range")
            start, end = 0, file_size - 1
            status_code = 200
            content_length = file_size

            if range_header:
                try:
                    unit, ranges = range_header.split("=", 1)
                    if unit == "bytes":
                        r_start, r_end = ranges.split("-", 1)
                        start = int(r_start) if r_start else 0
                        if r_end:
                            end = int(r_end)
                        # 修正长度
                        if start >= file_size:
                            return Response(status_code=416) # Range Not Satisfiable
                        content_length = end - start + 1
                        status_code = 206 # Partial Content
                except Exception:
                    pass # 如果 Range 解析失败，降级为全量下载

            headers = {
                "Content-Disposition": f"attachment; filename*=UTF-8''{file_name}",
                "Content-Length": str(content_length),
                "Last-Modified": last_mod,
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes {start}-{end}/{file_size}"
            }

            return StreamingResponse(
                self.r_stream(fp, start=start, length=content_length),
                status_code=status_code,
                media_type=mimetypes.guess_type(p)[0] or "application/octet-stream",
                headers=headers
            )
        except FileNotFoundError:
            return Response(status_code=404)
        except Exception:
            return Response(status_code=500)

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
        try:
            dp = unquote(urlparse(d_h).path).strip('/')
            sf = self._p(s)
            df = self._p(dp)

            await run_in_threadpool(self._chk, df)
            
            # 使用更安全的流式复制
            def _core():
                with self.fs.open(sf, 'rb') as f1:
                    with self.fs.open(df, 'wb') as f2:
                        while True:
                            b = f1.read(1024 * 1024) # 1MB Chunk
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
        x = f"""<?xml version="1.0" encoding="utf-8" ?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:owner><D:href>SysAdmin</D:href></D:owner><D:timeout>Second-3600</D:timeout><D:locktoken><D:href>{t}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>"""
        return Response(content=x, status_code=200, media_type="application/xml; charset=utf-8", headers={"Lock-Token": f"<{t}>"})

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

@app.get("/")
async def sys_status():
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
            <div class="footer">Node ID:
