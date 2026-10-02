"""node_monitor.web.socket -- race-free AF_UNIX socket lifecycle.

Security contract (spec §9):
- Parent run directory: mode 0700, owned by current euid (create or verify).
- Reject any pre-existing path that is a symlink.
- Reject any pre-existing path that is not a socket.
- Reject any socket owned by a foreign uid (fail-closed).
- Probe-and-remove operator-owned socket only when connect conclusively fails.
- A live socket (connect succeeds) raises "socket is already in use"; the
  inode is never disturbed.
- Create AF_UNIX/SOCK_STREAM under umask 0177 (kernel creates mode 0600).
- Immediately chmod 0600 and _verify_socket() BEFORE calling listen().
- No accept loop starts before verification succeeds.
- Cleanup on failure: unlink only the exact inode created by this process.
"""

import errno
import os
import socket
import stat


class SocketError(OSError):
   """Raised for any socket lifecycle policy violation."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def bind_private_socket(path, backlog=128):
   """Create, secure, and return a listening AF_UNIX socket at *path*.

   The returned socket is in the listening state.  The caller owns it and
   must call sock.close() when done.

   Raises SocketError (or a subclass of OSError) on any policy violation.
   """
   run_dir = os.path.dirname(path)
   _ensure_private_run_directory(run_dir)
   _prepare_absent_or_stale_socket(path)

   sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   old_umask = os.umask(0o177)
   try:
      sock.bind(path)
   finally:
      os.umask(old_umask)

   # Record the filesystem inode immediately after bind so we can match it
   # precisely during cleanup (fstat on the fd gives the socket kernel inode,
   # not the filesystem inode, on macOS/BSD).
   try:
      bound_ino = os.lstat(path).st_ino
   except OSError:
      bound_ino = None

   try:
      os.chmod(path, 0o600)
      _verify_socket(path, expected_uid=os.geteuid(), expected_mode=0o600)
      sock.listen(backlog)
      return sock
   except BaseException:
      sock.close()
      _unlink_if_same_fs_inode(path, bound_ino)
      raise


# ---------------------------------------------------------------------------
# Run-directory management
# ---------------------------------------------------------------------------

def _ensure_private_run_directory(run_dir):
   """Create run_dir mode 0700 if absent; verify mode and ownership if present."""
   if not os.path.exists(run_dir):
      os.makedirs(run_dir, mode=0o700, exist_ok=True)
      return

   info = os.stat(run_dir)
   if info.st_uid != os.geteuid():
      raise SocketError(
         "run directory %r is not owned by current user (uid %d, owner uid %d)"
         % (run_dir, os.geteuid(), info.st_uid)
      )
   current_mode = stat.S_IMODE(info.st_mode)
   if current_mode != 0o700:
      # Attempt to repair mode when we own the directory
      os.chmod(run_dir, 0o700)


# ---------------------------------------------------------------------------
# Pre-existing path handling
# ---------------------------------------------------------------------------

def _prepare_absent_or_stale_socket(path):
   """Ensure the socket path is absent or stale before we bind.

   Rules:
   - Absent → nothing to do.
   - Symlink → reject (never follow or remove).
   - Regular file or directory → reject ("not a socket").
   - Foreign-owned socket → reject ("foreign").
   - Operator-owned socket, connect probe fails → stale → unlink.
   - Operator-owned socket, connect probe succeeds → live → raise "already in use".
   """
   try:
      link_info = os.lstat(path)
   except FileNotFoundError:
      return  # Nothing there; proceed.

   # Symlinks: never touch.
   if stat.S_ISLNK(link_info.st_mode):
      raise SocketError(
         "socket path %r is a symlink; refusing to bind" % path
      )

   # Non-socket files: reject.
   if not stat.S_ISSOCK(link_info.st_mode):
      raise SocketError(
         "%r exists and is not a socket (mode %o)" % (path, link_info.st_mode)
      )

   # Socket file: check ownership.
   if link_info.st_uid != os.geteuid():
      raise SocketError(
         "socket path %r is owned by a foreign uid (%d); refusing to bind"
         % (path, link_info.st_uid)
      )

   # Operator-owned socket: probe to distinguish live from stale.
   if _socket_accepts_connections(path):
      raise SocketError(
         "socket is already in use at %r; refusing to bind" % path
      )

   # Conclusively stale: unlink.
   os.unlink(path)


def _socket_accepts_connections(path):
   """Return True if a connect to *path* succeeds, False if it conclusively fails."""
   probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   probe.settimeout(1.0)
   try:
      probe.connect(path)
      probe.close()
      return True
   except (ConnectionRefusedError, FileNotFoundError):
      return False
   except OSError as exc:
      # ECONNREFUSED (111) and ENOENT are conclusive stale indicators.
      # Any other error (e.g. EAGAIN) is treated conservatively as "live".
      if exc.errno in (errno.ECONNREFUSED, errno.ENOENT, errno.EACCES):
         return False
      return True
   finally:
      try:
         probe.close()
      except OSError:
         pass


# ---------------------------------------------------------------------------
# Mode/ownership verification (called BEFORE listen)
# ---------------------------------------------------------------------------

def _verify_socket(path, expected_uid, expected_mode):
   """Verify that *path* has exactly *expected_mode* and is owned by *expected_uid*.

   Raises OSError if either check fails.  This function is called with the
   real file after chmod and BEFORE listen(); it is also the monkeypatch
   target in tests that verify listen() is not called on failure.
   """
   info = os.stat(path)
   actual_mode = stat.S_IMODE(info.st_mode)
   if actual_mode != expected_mode:
      raise SocketError(
         "socket %r has mode %04o, expected %04o"
         % (path, actual_mode, expected_mode)
      )
   if info.st_uid != expected_uid:
      raise SocketError(
         "socket %r is owned by uid %d, expected %d"
         % (path, info.st_uid, expected_uid)
      )


# ---------------------------------------------------------------------------
# Cleanup helper
# ---------------------------------------------------------------------------

def _unlink_if_same_fs_inode(path, bound_ino):
   """Unlink *path* only if its current filesystem inode matches *bound_ino*.

   *bound_ino* is the inode number recorded immediately after bind(), using
   os.lstat() on the socket path (not fstat on the fd, which returns the
   kernel socket inode on macOS/BSD, not the filesystem inode).

   This prevents removing a socket that was replaced by another process after
   our bind succeeded.
   """
   if bound_ino is None:
      return
   try:
      path_info = os.lstat(path)
   except OSError:
      return  # Already gone.
   if path_info.st_ino == bound_ino:
      try:
         os.unlink(path)
      except OSError:
         pass
