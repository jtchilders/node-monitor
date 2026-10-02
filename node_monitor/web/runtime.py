"""node_monitor.web.runtime -- Uvicorn integration using a pre-bound socket.

Contract (spec §9):
- Receives an already-bound, already-listened AF_UNIX socket object.
- Passes fd=sock.fileno() to uvicorn.Config; never passes uds=, host=, port=.
- server_header=False, proxy_headers=False, access_log=False.
- The Python socket object stays alive (open) until the server exits.
- Does not accept connections before this function is called.
"""

import uvicorn


def run_uvicorn(app, sock):
   """Start Uvicorn with the pre-bound Unix socket *sock*.

   *app*  -- ASGI application callable.
   *sock* -- A socket.socket in the listening state (AF_UNIX, SOCK_STREAM),
             created and secured by bind_private_socket().

   Blocks until the server exits.  The socket is NOT closed by this function;
   the caller retains ownership.
   """
   config = uvicorn.Config(
      app,
      fd=sock.fileno(),
      server_header=False,
      proxy_headers=False,
      access_log=False,
   )
   server = uvicorn.Server(config)
   server.run()
