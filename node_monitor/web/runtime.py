"""node_monitor.web.runtime -- TCP Uvicorn runtime (no Unix socket)."""
import uvicorn

def run_uvicorn(app, host, port):
   config = uvicorn.Config(
      app,
      host=host,
      port=port,
      server_header=False,
      proxy_headers=False,
      access_log=False,
   )
   server = uvicorn.Server(config)
   server.run()
