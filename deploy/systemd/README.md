# Deploying the collector daemon

The unit runs as a systemd user service, so it needs no root and lives on `$HOME`.

## Install

```bash
mkdir -m 700 -p ~/node-monitor/logs
mkdir -p ~/.config/systemd/user
cp deploy/systemd/node-monitor-daemon.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now node-monitor-daemon.service
loginctl show-user "$USER" --property=Linger     # must say Linger=yes
```

If linger is off, run `loginctl enable-linger "$USER"` (or `deploy/systemd/rearm.sh`, which
does all of the above except the copy) so the service survives logout and reboots.

`logs/` must exist before the unit starts: systemd opens `logs/daemon.log` itself, and a
missing directory fails every start with status 209/STDOUT.

### Permissions (once)

`state/` holds the ssh config the collector runs ssh with, and `logs/` and the spool hold
other users' command lines, so nobody else may write to the tree or read those two
directories. The daemon tightens `state/` and `logs/` to 0700 at every start and the unit
sets `UMask=0077`, but a checkout shared with a group should be fixed once by hand:

```bash
chmod -R g-w,o-w ~/node-monitor
chmod 700 ~/node-monitor/{state,logs}
```

## Checking

```bash
systemctl --user status node-monitor-daemon.service
node-monitor daemon status
tail -f ~/node-monitor/logs/daemon.log
```

## Stopping

```bash
systemctl --user stop node-monitor-daemon.service
```

`node-monitor daemon stop` refuses while systemd manages the daemon (systemd would restart
it); `--force` overrides that.

## Pitfalls

- systemd starts services with a clean environment, so the unit sets `PATH` explicitly.
  Anything else the daemon needs from your shell must be set in the unit or the config.
- The SSH key must be usable without a passphrase and without an agent: there is no
  `SSH_AUTH_SOCK` under systemd and ssh runs in batch mode, so it cannot prompt. An
  encrypted key fails even when its path is set as `ssh.identity_file`; use a dedicated
  unencrypted key (set its path as `ssh.identity_file` if it is not a default key name).
- Do not run a soak and the daemon at the same time: they take over each other's probes on
  the nodes.
- `logs/daemon.log` is appended to forever. Rotate or truncate it from time to time.
- The database may be down at start: `ExecStartPre` waits up to 60 s and then the daemon
  starts anyway, spooling until the database returns.
