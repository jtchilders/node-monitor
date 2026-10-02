"""Tests for race-free Unix-socket lifecycle in node_monitor.web.socket.

Security requirements verified here:
- run directory created mode 0700 and owned by current euid
- socket mode is exactly 0600 BEFORE listen() is called
- socket mode is verified after chmod and BEFORE listen()
- no accept loop starts before verification succeeds
- foreign-owned socket is rejected fail-closed
- operator-owned stale socket removed only after failed connect probe
- duplicate launch cannot disturb live inode
- pre-existing regular file is rejected
- pre-existing symlink is rejected
- cleanup unlinks only the exact inode created by this process

Note: macOS AF_UNIX path limit is 103 bytes. Tests use short /tmp paths
to stay within that limit (pytest's tmp_path uses longer paths under
/private/var/folders/...).
"""

import os
import shutil
import socket
import stat
import tempfile

import pytest

from node_monitor.web.socket import bind_private_socket


# ---------------------------------------------------------------------------
# Fixture: short-path temp directory that stays within macOS 103-byte limit
# ---------------------------------------------------------------------------

@pytest.fixture()
def short_tmp():
   """Yield a short absolute temp directory path and clean up after."""
   d = tempfile.mkdtemp(dir="/tmp")
   try:
      yield d
   finally:
      shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mode(path):
   return stat.S_IMODE(os.stat(path).st_mode)


def _lmode(path):
   return stat.S_IMODE(os.lstat(path).st_mode)


def _uid(path):
   return os.stat(path).st_uid


def _sock_path(base):
   """Return a socket path short enough for macOS AF_UNIX."""
   return os.path.join(base, "run", "w.sock")


# ---------------------------------------------------------------------------
# Test: run directory is created with mode 0700 and same euid
# ---------------------------------------------------------------------------

def test_run_directory_created_mode_0700(short_tmp):
   path = _sock_path(short_tmp)
   sock = bind_private_socket(path)
   try:
      assert _mode(os.path.join(short_tmp, "run")) == 0o700
   finally:
      sock.close()


def test_run_directory_owner_is_current_euid(short_tmp):
   path = _sock_path(short_tmp)
   sock = bind_private_socket(path)
   try:
      assert _uid(os.path.join(short_tmp, "run")) == os.geteuid()
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: socket is mode 0600 BEFORE listen() and after listen()
# ---------------------------------------------------------------------------

def test_socket_is_private_before_listen(short_tmp, monkeypatch):
   """Mode must be 0600 at the moment listen() is called (not after)."""
   events = []
   real_listen = socket.socket.listen

   def inspect_then_listen(sock, backlog=128):
      mode = stat.S_IMODE(os.stat(sock.getsockname()).st_mode)
      events.append(mode)
      return real_listen(sock, backlog)

   monkeypatch.setattr(socket.socket, "listen", inspect_then_listen)
   bound = bind_private_socket(_sock_path(short_tmp))
   try:
      assert events == [0o600], "expected mode 0600 at listen boundary, got %s" % events
   finally:
      bound.close()


def test_socket_is_mode_0600_after_listen(short_tmp):
   path = _sock_path(short_tmp)
   sock = bind_private_socket(path)
   try:
      assert _mode(path) == 0o600
   finally:
      sock.close()


def test_socket_owner_is_current_euid(short_tmp):
   path = _sock_path(short_tmp)
   sock = bind_private_socket(path)
   try:
      assert _uid(path) == os.geteuid()
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: listen() is never called if mode/ownership verification fails
# ---------------------------------------------------------------------------

def test_listen_not_called_if_verification_fails(short_tmp, monkeypatch):
   """If chmod verification would fail, listen() must not be called."""
   listen_called = []
   real_listen = socket.socket.listen

   def tracking_listen(sock, backlog=128):
      listen_called.append(True)
      return real_listen(sock, backlog)

   import node_monitor.web.socket as ws_module

   def always_fail(path, expected_uid, expected_mode):
      raise RuntimeError("injected verification failure")

   monkeypatch.setattr(socket.socket, "listen", tracking_listen)
   monkeypatch.setattr(ws_module, "_verify_socket", always_fail)

   with pytest.raises(RuntimeError, match="injected verification failure"):
      bind_private_socket(_sock_path(short_tmp))

   assert listen_called == [], "listen() must not be called before verification"


# ---------------------------------------------------------------------------
# Test: duplicate launch does not unlink the live socket
# ---------------------------------------------------------------------------

def test_duplicate_launch_does_not_unlink_live_socket(short_tmp):
   path = _sock_path(short_tmp)
   first = bind_private_socket(path)
   try:
      with pytest.raises(Exception, match="socket is already in use"):
         bind_private_socket(path)
      # Live socket inode must still be present and be a socket
      assert stat.S_ISSOCK(os.stat(path).st_mode)
   finally:
      first.close()


# ---------------------------------------------------------------------------
# Test: pre-existing regular file is rejected fail-closed
# ---------------------------------------------------------------------------

def test_rejects_preexisting_regular_file(short_tmp):
   run_dir = os.path.join(short_tmp, "run")
   os.makedirs(run_dir, mode=0o700)
   path = os.path.join(run_dir, "w.sock")
   # Place a regular file at the socket path
   with open(path, "w") as fh:
      fh.write("not a socket")
   with pytest.raises(Exception, match="not a socket"):
      bind_private_socket(path)
   # File must not have been removed
   assert os.path.exists(path)


# ---------------------------------------------------------------------------
# Test: pre-existing symlink is rejected fail-closed
# ---------------------------------------------------------------------------

def test_rejects_preexisting_symlink(short_tmp):
   run_dir = os.path.join(short_tmp, "run")
   os.makedirs(run_dir, mode=0o700)
   path = os.path.join(run_dir, "w.sock")
   target = os.path.join(short_tmp, "target.sock")
   os.symlink(target, path)
   with pytest.raises(Exception, match="symlink"):
      bind_private_socket(path)
   # Symlink must not have been removed
   assert os.path.islink(path)


# ---------------------------------------------------------------------------
# Test: foreign-owned socket is rejected fail-closed
# ---------------------------------------------------------------------------

def test_rejects_foreign_owned_socket(short_tmp, monkeypatch):
   """A socket owned by a different uid must be rejected without removal."""
   run_dir = os.path.join(short_tmp, "run")
   os.makedirs(run_dir, mode=0o700)
   path = os.path.join(run_dir, "w.sock")

   # Create a real socket file at the path to simulate pre-existing socket
   existing = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   old_umask = os.umask(0o177)
   try:
      existing.bind(path)
   finally:
      os.umask(old_umask)
   existing.close()

   # Inject a stat result that reports a foreign uid
   real_lstat = os.lstat
   foreign_uid = os.geteuid() + 1

   def fake_lstat(p):
      result = real_lstat(p)
      if p == path:
         class FakeStat:
            st_mode = result.st_mode
            st_uid = foreign_uid
            st_ino = result.st_ino
         return FakeStat()
      return result

   monkeypatch.setattr(os, "lstat", fake_lstat)

   with pytest.raises(Exception, match="foreign"):
      bind_private_socket(path)

   # The socket file must still exist (not unlinked by us)
   assert os.path.exists(path)


# ---------------------------------------------------------------------------
# Test: operator-owned stale socket removed only after failed connect probe
# ---------------------------------------------------------------------------

def test_stale_operator_socket_removed_after_failed_probe(short_tmp):
   """Operator-owned socket with no listener → connect probe fails → remove."""
   run_dir = os.path.join(short_tmp, "run")
   os.makedirs(run_dir, mode=0o700)
   path = os.path.join(run_dir, "w.sock")

   # Create a socket file with no listener (stale)
   stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
   old_umask = os.umask(0o177)
   try:
      stale.bind(path)
   finally:
      os.umask(old_umask)
   os.chmod(path, 0o600)
   stale.close()
   # Do NOT call listen() — socket exists but is not accepting connections

   # bind_private_socket should detect stale socket, probe, and remove it
   sock = bind_private_socket(path)
   try:
      assert stat.S_ISSOCK(os.stat(path).st_mode)
      assert _mode(path) == 0o600
   finally:
      sock.close()


# ---------------------------------------------------------------------------
# Test: live operator-owned socket raises "already in use"
# ---------------------------------------------------------------------------

def test_live_operator_socket_raises_already_in_use(short_tmp):
   """Operator-owned socket that accepts connections → must raise, not remove."""
   path = _sock_path(short_tmp)
   live = bind_private_socket(path)
   inode_before = os.stat(path).st_ino
   try:
      with pytest.raises(Exception, match="socket is already in use"):
         bind_private_socket(path)
      # Same inode must still be there
      assert os.stat(path).st_ino == inode_before
   finally:
      live.close()


# ---------------------------------------------------------------------------
# Test: run directory with wrong ownership is rejected
# ---------------------------------------------------------------------------

def test_rejects_run_directory_owned_by_foreign_uid(short_tmp, monkeypatch):
   """If the run directory exists and is owned by a different uid, fail closed."""
   run_dir = os.path.join(short_tmp, "run")
   os.makedirs(run_dir, mode=0o700)
   path = os.path.join(run_dir, "w.sock")

   real_stat = os.stat
   foreign_uid = os.geteuid() + 1

   def fake_stat(p, **kwargs):
      result = real_stat(p, **kwargs)
      if p == run_dir:
         class FakeStat:
            st_mode = result.st_mode
            st_uid = foreign_uid
            st_ino = result.st_ino
         return FakeStat()
      return result

   monkeypatch.setattr(os, "stat", fake_stat)

   with pytest.raises(Exception, match="not owned"):
      bind_private_socket(path)


# ---------------------------------------------------------------------------
# Test: cleanup unlinks only the exact inode created by this process
# ---------------------------------------------------------------------------

def test_cleanup_unlinks_only_own_inode(short_tmp, monkeypatch):
   """On failure after bind but before listen, only our inode is cleaned up."""
   path = _sock_path(short_tmp)

   import node_monitor.web.socket as ws_module

   unlinked = []
   real_unlink = os.unlink

   def tracking_unlink(p):
      unlinked.append(p)
      return real_unlink(p)

   monkeypatch.setattr(os, "unlink", tracking_unlink)

   # Make verification fail so cleanup runs
   def always_fail(p, expected_uid, expected_mode):
      raise RuntimeError("injected")

   monkeypatch.setattr(ws_module, "_verify_socket", always_fail)

   with pytest.raises(RuntimeError):
      bind_private_socket(path)

   # The socket path should have been unlinked exactly once (our inode)
   assert path in unlinked, "expected our socket path to be cleaned up"
   # No other paths should have been unlinked
   assert all(u == path for u in unlinked), "unexpected extra unlinks: %s" % unlinked


# ---------------------------------------------------------------------------
# Test: returned socket is AF_UNIX SOCK_STREAM and is bound
# ---------------------------------------------------------------------------

def test_returned_socket_is_af_unix_sock_stream(short_tmp):
   path = _sock_path(short_tmp)
   sock = bind_private_socket(path)
   try:
      assert sock.family == socket.AF_UNIX
      assert sock.type == socket.SOCK_STREAM
      assert sock.getsockname() == path
   finally:
      sock.close()
