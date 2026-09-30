"""Legacy/bootstrap SQL artifact for node-monitor source tables.

Runtime ownership of database engines, migrations, and writers belongs solely
under :mod:`node_monitor.database`. ``schema.sql`` remains only for legacy
provisioning compatibility; it is not a migration runner or a second database
implementation.
"""
