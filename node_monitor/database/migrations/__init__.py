"""node_monitor.database.migrations -- packaged SQL migration resources.

Discovery lives in ``node_monitor.database.migration`` (singular); this
package and its ``versions`` subpackage exist only to hold the packaged
``.sql`` resource files so ``importlib.resources`` can find them under
both a normal install and an editable install.
"""
