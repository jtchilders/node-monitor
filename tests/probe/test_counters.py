from tests.probe import fake_proc


def test_counters_parse_fake_tree(probe, tmp_path):
   roots = fake_proc.write_tree(
      tmp_path,
      loadavg="1.50 2.00 2.50 7/6616 3371728\n",
      stat_cpu="cpu  100 5 50 1000 20 1 2 0 0 0\n",
      meminfo={"MemTotal": 528000000, "MemAvailable": 400000000},
      net_dev={"hsn0": (1000, 2000), "lo": (5, 5)},
      md_stats={"eagle-MDT0000-mdc-ff": {"open": 10, "close": 9}},
      sockstat_used=812,
   )
   c = probe.collect_counters(roots)
   assert (c["load1"], c["procs_running"], c["procs_total"]) == (1.5, 7, 6616)
   assert c["cpu_jiffies"]["iowait"] == 20
   assert c["mem"]["available_kb"] == 400000000
   assert c["net"] == {"hsn0": {"rx_bytes": 1000, "tx_bytes": 2000}}
   assert c["md_ops"]["eagle-MDT0000-mdc-ff"]["total"] == 19
   assert c["socket_count"] == 812
   assert c["uptime_sec"] == 340091.15


def test_counters_tolerate_missing_files(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path, omit=("loadavg", "net/dev", "fs/lustre"))
   c = probe.collect_counters(roots)
   assert "load1" not in c and c["net"] == {} and c["md_ops"] == {}


def test_hardware_and_boot_identity(probe, tmp_path):
   roots = fake_proc.write_tree(
      tmp_path, boot_id="abc-123", btime=1760000000, cpuinfo_processors=4,
      cpu_model="AMD EPYC 7543P",
   )  # fmt: skip
   hw = probe.collect_hardware(roots)
   assert hw["cpu_logical"] == 4 and hw["cpu_model"] == "AMD EPYC 7543P"
   assert probe.boot_identity(roots) == ("abc-123", 1760000000)


def test_hardware_inventory_fields(probe, tmp_path):
   roots = fake_proc.write_tree(tmp_path)
   hw = probe.collect_hardware(roots)
   assert hw["numa_nodes"] == 1
   assert hw["cpu_max_freq_khz"] == 3700000
   assert hw["net_fs_mounts"] == 2
   assert hw["net_ifaces"] == {"hsn0": 200000}
   assert hw["kernel_release"] == "5.14.21-test"
   assert hw["hugepage_size_kb"] == 2048


def test_read_text_returns_none_for_vanished(probe, tmp_path):
   assert probe.read_text(str(tmp_path / "nope")) is None


def test_read_link_returns_none_on_error(probe, tmp_path):
   assert probe.read_link(str(tmp_path / "nope")) is None
