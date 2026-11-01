# Deploying the collector daemon

The unit runs as a systemd user service, so it needs no root and lives on `$HOME`.

## Install

```bash
mkdir -p ~/node-monitor/logs
mkdir -p ~/.config/systemd/user
cp deploy/systemd/node-monitor-daemon.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now node-monitor-daemon.service
loginctl show-user "$USER" --property=Linger     # must say Linger=yes
```

If linger is off, run `loginctl enable-linger "$USER"` (or `deploy/systemd/rearm.sh`, which
does all of the above except the copy) so the service survives logout and reboots.

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
- The SSH key must work without an agent: there is no `SSH_AUTH_SOCK` under systemd. Use an
  unencrypted key or one whose path is set as `ssh.identity_file`.
- Do not run a soak and the daemon at the same time: they take over each other's probes on
  the nodes.
- `logs/daemon.log` is appended to forever. Rotate or truncate it from time to time.
- The database may be down at start: `ExecStartPre` waits up to 60 s and then the daemon
  starts anyway, spooling until the database returns.
