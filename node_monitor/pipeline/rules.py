"""Process classification rule tables, tuned against real login-node observations.

Matching is highest-priority-wins (see classify.py): only priority decides overlaps; ties
go to the earlier rule.
"""

SPEC_CATEGORIES = frozenset(
   {
      "ai_agent",
      "editor_server",
      "jupyter",
      "build",
      "python",
      "shell",
      "data_transfer",
      "fs_scan",
      "scheduler",
      "workflow",
      "system",
      "other",
   }
)

DEFAULT_CATEGORY = "other"

# (category, regex, priority): fallback when no activity rule matches.
CATEGORY_RULES = (
   (
      "editor_server",
      r"vscode-server|vscode_server|\.vscode-server|cursor-server|"
      r"\.cursor-server|jetbrains|remote-dev-server|code-server",
      10,
   ),
   # Priority 20 > editor_server: an agent marker inside an editor-server path (e.g.
   # .vscode-server/extensions/anthropic.claude-code-*/bin/claude) must classify as the agent.
   (
      "ai_agent",
      r"(^|/)(claude|codex|opencode|aider|gemini|cline|goose|windsurf|"
      r"hermes|hermes-agent|hermes_cli|openclaw|open-claw|open_claw)"
      r"( |$)|"
      r"-m\s+(claude|codex|opencode|aider|gemini|cline|goose|windsurf|"
      r"hermes_cli|openclaw)[_.\s]|"
      r"anthropic\.claude-code|openai\.chatgpt|claude-code|claude_code|"
      r"copilot-language|copilot-agent|gh-copilot|github\.copilot|"
      r"codeium-windsurf|codeium\.windsurf|windsurf-server|"
      r"\.windsurf-server|cursor-agent|"
      r"roo-cline|roo-code|rooveterinaryinc\.roo-cline|"
      r"continue\.continue|continuedev|\.continue/",
      20,
   ),
   ("jupyter", r"jupyter|ipykernel|jupyter-lab|jupyterhub", 10),
   (
      "scheduler",
      r"(^|/)(qstat|qsub|qdel|qhold|qrls|qalter|pbsnodes|pbs_[a-z]+|"
      r"mpiexec|mpirun|aprun|srun)( |$)",
      10,
   ),
   (
      "data_transfer",
      r"(^|/)(git|git-[a-z-]+|rsync|scp|sftp|globus|globus-url-copy|curl|"
      r"wget|tar|gzip|gunzip|bzip2|xz|zip|unzip|hsi|htar)( |$)",
      10,
   ),
   (
      "fs_scan",
      r"(^|/)(find|du|ncdu|updatedb|locate|stat|ls)( |$)|"
      r"(^|/)lfs( |$).*(find|df)|(^|/)mlocate",
      10,
   ),
   (
      "build",
      r"(^|/)(make|gmake|cmake|ninja|gcc|g\+\+|cc|c\+\+|clang|clang\+\+|"
      r"nvcc|ftn|CC|ld|ar|ranlib|cargo|rustc|go|javac|configure|conda|pip|"
      r"pip3|spack)( |$)",
      10,
   ),
   ("python", r"(^|/)(python|python[0-9.]*)( |$)", 10),
   (
      "shell",
      r"(^|/)(sshd|bash|zsh|tcsh|csh|ksh|sh|screen|tmux|systemd|"
      r"\(sd-pam\)|dbus-daemon|dbus-launch|login|su|sudo)( |:|$)|^-",
      10,
   ),
)

# (activity, category, regex, priority)
ACTIVITY_RULES = (
   ("vscode-remote-server", "editor_server", r"vscode-server|code-server", 10),
   ("cursor-remote-server", "editor_server", r"cursor-server", 10),
   # Priority 20 > vscode-remote-server for extension paths such as
   # .vscode-server/extensions/anthropic.claude-code-*/bin/claude.
   (
      "claude-code",
      "ai_agent",
      r"claude-code|anthropic\.claude-code|(^|/)claude( |$)",
      20,
   ),
   ("codex-cli", "ai_agent", r"(^|/)codex( |$)|openai\.chatgpt", 20),
   # Bare names are boundary-anchored so "my_windsurf_notes" and "/data/hermes/results"
   # do not match.
   (
      "windsurf",
      "ai_agent",
      r"(^|/)windsurf( |$)|codeium-windsurf|codeium\.windsurf|"
      r"windsurf-server|\.windsurf-server",
      20,
   ),
   ("cursor-cli", "ai_agent", r"cursor-agent|(^|/)cursor( |$)", 20),
   ("hermes", "ai_agent", r"hermes-agent|hermes_cli|(^|/)hermes( |$)", 20),
   ("openclaw", "ai_agent", r"open-claw|openclaw|open_claw", 20),
   # Bare "roo" and "continue" are too collision-prone; only compound markers count.
   ("roo", "ai_agent", r"roo-cline|roo-code|rooveterinaryinc\.roo-cline", 20),
   ("continue", "ai_agent", r"continue\.continue|continuedev|(^|/)\.continue/", 20),
   (
      "copilot-agent",
      "ai_agent",
      r"copilot-agent|copilot-language-server|gh-copilot|github\.copilot",
      20,
   ),
   ("jupyter-kernel", "jupyter", r"ipykernel", 10),
   ("jupyter-server", "jupyter", r"jupyter", 10),
   ("torch-distributed", "python", r"torch\.distributed|torchrun", 10),
   ("mpi-launch", "scheduler", r"(^|/)(mpiexec|mpirun|aprun|srun)( |$)", 10),
   (
      "compiler-invocation",
      "build",
      r"(^|/)(gcc|g\+\+|clang|nvcc|ftn|cc|c\+\+)( |$)",
      10,
   ),
   ("build-driver", "build", r"(^|/)(make|gmake|cmake|ninja)( |$)", 10),
   ("package-install", "build", r"(^|/)(pip|pip3|conda|spack)( |$)", 10),
   (
      "git-operation",
      "data_transfer",
      r"(^|/)git( |$)|(^|/)git-[a-z-]+( |$)",
      10,
   ),
   (
      "data-transfer",
      "data_transfer",
      r"(^|/)(rsync|scp|sftp|globus|globus-url-copy|curl|wget)( |$)",
      10,
   ),
   ("filesystem-scan", "fs_scan", r"(^|/)(find|du|ncdu|updatedb)( |$)", 10),
   ("pbs-query", "scheduler", r"(^|/)(qstat|qsub|qdel|pbsnodes)( |$)", 10),
   (
      "globus-compute",
      "workflow",
      r"globus-compute-endpoint|globus.compute|gc_submit|gc-ctrl",
      10,
   ),
   ("balsam", "workflow", r"(^|/)balsam( |$)|-m\s+balsam\.", 10),
   ("parsl", "workflow", r"(^|/)parsl( |$)|-m\s+parsl\.", 10),
   (
      "argo",
      "workflow",
      r"argo-shim|argo-proxy|argo.bridge|ARGO_BRIDGE_SOCK",
      10,
   ),
   ("postgres", "system", r"(^|/)postgres( |$)|postmaster", 10),
   ("dbus", "system", r"dbus-daemon|dbus-launch|dbus-session", 10),
   ("fail2ban", "system", r"fail2ban-server|fail2ban", 10),
   ("ssh-agent", "shell", r"(^|/)ssh-agent( |$)", 10),
   ("sleep", "shell", r"(^|/)sleep( |$)", 10),
   ("pytest", "python", r"-m\s+pytest\b", 10),
   ("reuse-lint", "other", r"-m\s+reuse\b", 10),
   (
      "python-http-server",
      "python",
      r"-m\s+http\.server|preview_server|serve_boards|"
      r"helios_preview_server|nxserver",
      10,
   ),
   ("starccm", "other", r"-m\s+starserver|STAR-CCM", 10),
   (
      "python-training",
      "python",
      r"(^|[/ ])train\.py( |$)|train_.*\.py|"
      r"finetune|fine.tune",
      10,
   ),
   (
      "python-inference",
      "python",
      r"(^|[/ ])infer\.py( |$)|infer_.*\.py|"
      r"(^|[/ ])predict\.py( |$)|generat.*\.py|"
      r"(^|[/ ])boltz( |$)",
      10,
   ),
   # Catch-all for python processes nothing above claims; low priority so any specific
   # rule wins.
   ("python-generic", "python", r"(^|/)python[0-9.]*( |$)", 5),
   ("shell", "shell", r"(^|/)(bash|zsh|tcsh|csh|ksh|sh)( |$)|^-", 10),
   ("ssh-session", "shell", r"(^|/)sshd( |:|$)", 10),
   ("terminal-multiplexer", "shell", r"(^|/)(screen|tmux)( |$)", 10),
   # Priority 15, between the broad editor rule (10) and named agents (20): a novel agent
   # extension under .vscode-server surfaces here, a known one keeps its own label, and an
   # ordinary extension (linter etc.) stays vscode-remote-server.
   (
      "agent-like-unclassified",
      "ai_agent",
      r"(^|/)\.vscode-server/extensions/[A-Za-z0-9_-]+\."
      r"[A-Za-z0-9_.-]*(?:agent|assistant|copilot|chatbot)"
      r"[A-Za-z0-9_.-]*(?:/|$)",
      15,
   ),
)
