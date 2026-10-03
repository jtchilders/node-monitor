"""Exact static allowlist with one helper, context-managed reads."""
import importlib.resources as res

STATIC_ROUTES = {
   "/static/index.html": ("index.html", "text/html"),
   "/static/styles.css": ("styles.css", "text/css"),
   "/static/app.js": ("app.js", "application/javascript"),
   "/static/chart.umd.min.js": ("chart.umd.min.js", "application/javascript"),
}
ALLOWLIST = {"index.html", "styles.css", "app.js", "chart.umd.min.js"}


def read_static(name):
   if name not in ALLOWLIST:
      raise ValueError("static name not in allowlist: %r" % name)
   with (res.files("node_monitor.web") / "static" / name).open("rb") as f:
      return f.read()
