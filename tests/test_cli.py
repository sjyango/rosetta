"""
Regression tests for rosetta CLI commands.

Covers: status, exec, result (list/show), config (show/init/validate),
        global options (-j/--json position), and argument parsing.

Tests use the real CLI entry point (rosetta.cli.main.main) with JSON output
for easy assertion, avoiding any DB connections by mocking where needed.
"""

import json
import os
import shutil
import tempfile
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_cli(*argv: str) -> dict:
    """Run rosetta CLI with the given argv and return parsed JSON output."""
    from rosetta.cli.main import main
    from io import StringIO

    # Always inject -j for machine-parsable output
    full_argv = list(argv)
    if "-j" not in full_argv and "--json" not in full_argv:
        full_argv.insert(0, "-j")

    buf = StringIO()
    with mock.patch("sys.stdout", buf):
        exit_code = main(full_argv)

    output = buf.getvalue().strip()
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        pytest.fail(f"CLI did not produce valid JSON.\nexit={exit_code}\noutput={output!r}")


def run_cli_human(*argv: str) -> str:
    """Run rosetta CLI with human output and return stdout text."""
    from rosetta.cli.main import main
    from io import StringIO

    full_argv = list(argv)

    buf = StringIO()
    with mock.patch("sys.stdout", buf):
        main(full_argv)

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_dir():
    """Create a temporary directory and clean up after test."""
    d = tempfile.mkdtemp(prefix="rosetta_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture()
def sample_config(tmp_dir):
    """Write a minimal dbms_config.json and return its path."""
    cfg = {
        "databases": [
            {
                "name": "testdb1",
                "host": "127.0.0.1",
                "port": 39999,
                "user": "testuser",
                "password": "testpass",
                "driver": "pymysql",
                "enabled": True,
            },
            {
                "name": "testdb2",
                "host": "127.0.0.1",
                "port": 39998,
                "user": "testuser",
                "password": "testpass",
                "driver": "pymysql",
                "enabled": False,
            },
        ]
    }
    path = os.path.join(tmp_dir, "dbms_config.json")
    with open(path, "w") as f:
        json.dump(cfg, f)
    return path


@pytest.fixture()
def sample_results_dir(tmp_dir):
    """Create a fake results directory with bench and mtr runs."""
    results_dir = os.path.join(tmp_dir, "results")

    # bench run
    bench_dir = os.path.join(results_dir, "bench_test_20260401_100000")
    os.makedirs(bench_dir)
    bench_result = {
        "mode": "SERIAL",
        "dbms_results": [
            {
                "dbms_name": "mysql",
                "overall_qps": 100.5,
                "total_duration_s": 10.0,
                "total_queries": 1000,
                "total_errors": 0,
                "query_stats": [],
            }
        ],
    }
    with open(os.path.join(bench_dir, "bench_result.json"), "w") as f:
        json.dump(bench_result, f)
    with open(os.path.join(bench_dir, "bench_test.html"), "w") as f:
        f.write("<html></html>")
    with open(os.path.join(bench_dir, "bench_test.report.txt"), "w") as f:
        f.write("report")

    # mtr run
    mtr_dir = os.path.join(results_dir, "mtr_test_20260401_090000")
    os.makedirs(mtr_dir)
    with open(os.path.join(mtr_dir, "test.mysql.result"), "w") as f:
        f.write("ok")
    with open(os.path.join(mtr_dir, "test.tdsql.result"), "w") as f:
        f.write("ok")
    with open(os.path.join(mtr_dir, "test.html"), "w") as f:
        f.write("<html></html>")

    # older bench run
    bench_dir2 = os.path.join(results_dir, "bench_old_20260301_080000")
    os.makedirs(bench_dir2)
    with open(os.path.join(bench_dir2, "bench_result.json"), "w") as f:
        json.dump(bench_result, f)

    return results_dir


# ===========================================================================
# 1. Global options / argument parsing
# ===========================================================================

class TestGlobalOptions:
    """Test that global CLI options work in different positions."""

    def test_json_before_subcommand(self, sample_config):
        result = run_cli("-j", "status", "--config", sample_config)
        assert result["ok"] is True
        assert "data" in result

    def test_json_after_subcommand(self, sample_config):
        result = run_cli("status", "-j", "--config", sample_config)
        assert result["ok"] is True

    def test_json_long_form(self, sample_config):
        result = run_cli("status", "--json", "--config", sample_config)
        assert result["ok"] is True

    def test_version_json(self):
        result = run_cli("--version")
        assert result["ok"] is True
        assert result["command"] == "version"
        assert result["data"]["name"] == "rosetta"
        assert result["data"]["version"] == "1.0.0"

    def test_version_short_flag(self):
        output = run_cli_human("-V")
        assert output.strip() == "rosetta 1.0.0"

    def test_version_legacy_lower_v(self):
        output = run_cli_human("-v")
        assert output.strip() == "rosetta 1.0.0"

    def test_version_human(self):
        output = run_cli_human("--version")
        assert output.strip() == "rosetta 1.0.0"

    def test_no_command_shows_help(self):
        """rosetta with no args should print help and exit 0."""
        from rosetta.cli.main import main
        from io import StringIO
        buf = StringIO()
        with mock.patch("sys.stdout", buf):
            code = main([])
        assert code == 0

    def test_verbose_flag(self, sample_config):
        result = run_cli("-j", "-v", "status", "--config", sample_config)
        assert result["ok"] is True


# ===========================================================================
# 2. status command
# ===========================================================================

class TestStatusCommand:

    def test_status_json(self, sample_config):
        result = run_cli("status", "--config", sample_config)
        assert result["ok"] is True
        data = result["data"]
        assert "total" in data
        assert "connected" in data
        assert "dbms" in data
        # testdb2 is disabled, should only see testdb1
        assert data["total"] == 1

    def test_status_human(self, sample_config):
        output = run_cli_human("status", "--config", sample_config)
        assert "testdb1" in output

    def test_status_missing_config(self, tmp_dir):
        fake = os.path.join(tmp_dir, "nonexistent.json")
        result = run_cli("status", "--config", fake)
        assert result["ok"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 3. exec command
# ===========================================================================

class TestExecCommand:

    def test_exec_requires_sql_or_file(self, sample_config):
        result = run_cli("exec", "--config", sample_config, "--dbms", "testdb1")
        assert result["ok"] is False
        assert "required" in result["error"].lower()

    def test_exec_file_not_found(self, sample_config):
        result = run_cli("exec", "--config", sample_config,
                         "--dbms", "testdb1", "--file", "/nonexistent.sql")
        assert result["ok"] is False
        assert "not found" in result["error"].lower()

    def test_exec_sql_unreachable(self, sample_config):
        """exec with unreachable port should report error per DBMS."""
        result = run_cli("exec", "--config", sample_config,
                         "--dbms", "testdb1",
                         "--sql", "SELECT 1")
        assert result["ok"] is True
        data = result["data"]
        assert "results" in data
        # Port 39999 is not open, so should have error
        testdb1 = data["results"].get("testdb1", {})
        assert testdb1.get("error") is not None

    def test_exec_with_file(self, sample_config, tmp_dir):
        """exec --file with unreachable DB still returns structured result."""
        sql_file = os.path.join(tmp_dir, "test.sql")
        with open(sql_file, "w") as f:
            f.write("SELECT 1;\nSELECT 2;\n")
        result = run_cli("exec", "--config", sample_config,
                         "--dbms", "testdb1", "--file", sql_file)
        assert result["ok"] is True
        data = result["data"]
        assert data["total_statements"] == 2

    def test_exec_missing_config(self, tmp_dir):
        fake = os.path.join(tmp_dir, "nope.json")
        result = run_cli("exec", "--config", fake,
                         "--dbms", "x", "--sql", "SELECT 1")
        assert result["ok"] is False

    def test_exec_unknown_dbms(self, sample_config):
        result = run_cli("exec", "--config", sample_config,
                         "--dbms", "nonexistent_db", "--sql", "SELECT 1")
        assert result["ok"] is False


# ===========================================================================
# 4. result command (list / show)
# ===========================================================================

class TestResultCommand:

    def test_result_list_default(self, sample_results_dir):
        result = run_cli("result", "list",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is True
        data = result["data"]
        assert data["total"] == 3
        assert len(data["runs"]) == 3

    def test_result_list_pagination(self, sample_results_dir):
        result = run_cli("result", "list",
                         "--output-dir", sample_results_dir,
                         "-n", "2", "-p", "1")
        data = result["data"]
        assert data["showing"] == 2
        assert data["page"] == 1
        assert data["total_pages"] == 2

    def test_result_list_page2(self, sample_results_dir):
        result = run_cli("result", "list",
                         "--output-dir", sample_results_dir,
                         "-n", "2", "-p", "2")
        data = result["data"]
        assert data["showing"] == 1
        assert data["page"] == 2

    def test_result_list_filter_bench(self, sample_results_dir):
        result = run_cli("result", "list",
                         "--output-dir", sample_results_dir,
                         "--type", "bench")
        data = result["data"]
        for run in data["runs"]:
            assert run["type"] == "bench"
        assert data["total"] == 2

    def test_result_list_filter_mtr(self, sample_results_dir):
        result = run_cli("result", "list",
                         "--output-dir", sample_results_dir,
                         "--type", "mtr")
        data = result["data"]
        assert data["total"] == 1
        assert data["runs"][0]["type"] == "mtr"

    def test_result_list_empty_dir(self, tmp_dir):
        empty = os.path.join(tmp_dir, "empty_results")
        os.makedirs(empty)
        result = run_cli("result", "list", "--output-dir", empty)
        assert result["ok"] is True
        assert result["data"]["total"] == 0

    def test_result_default_is_list(self, sample_results_dir):
        """'rosetta result list' should return runs."""
        result = run_cli("result", "list", "--output-dir", sample_results_dir)
        assert result["ok"] is True
        assert "runs" in result["data"]

    def test_result_show_latest(self, sample_results_dir):
        result = run_cli("result", "show",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is True
        data = result["data"]
        assert "run_id" in data
        assert "path" in data
        # Path should be absolute
        assert os.path.isabs(data["path"])

    def test_result_show_specific(self, sample_results_dir):
        result = run_cli("result", "show", "bench_test_20260401_100000",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is True
        data = result["data"]
        assert data["run_id"] == "bench_test_20260401_100000"
        assert data["type"] == "bench"
        assert "bench_summary" in data
        assert len(data["bench_summary"]) == 1
        assert data["bench_summary"][0]["qps"] == 100.5

    def test_result_show_prefix_match(self, sample_results_dir):
        result = run_cli("result", "show", "bench_test_2026",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is True
        assert result["data"]["run_id"] == "bench_test_20260401_100000"

    def test_result_show_not_found(self, sample_results_dir):
        result = run_cli("result", "show", "nonexistent_run",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is False

    def test_result_show_mtr(self, sample_results_dir):
        result = run_cli("result", "show", "mtr_test_20260401_090000",
                         "--output-dir", sample_results_dir)
        assert result["ok"] is True
        data = result["data"]
        assert data["type"] == "mtr"
        assert "mysql" in data["dbms"]
        assert "tdsql" in data["dbms"]

    def test_result_show_report_files_absolute(self, sample_results_dir):
        result = run_cli("result", "show", "bench_test_20260401_100000",
                         "--output-dir", sample_results_dir)
        data = result["data"]
        for f in data["report_files"]:
            assert os.path.isabs(f)

    def test_result_show_human(self, sample_results_dir):
        output = run_cli_human("result", "show", "bench_test_20260401_100000",
                               "--output-dir", sample_results_dir)
        assert "bench_test_20260401_100000" in output
        assert "QPS" in output or "100.5" in output


# ===========================================================================
# 5. config command
# ===========================================================================

class TestConfigCommand:

    def test_config_show(self, sample_config):
        result = run_cli("config", "show", "--config", sample_config)
        assert result["ok"] is True
        data = result["data"]
        assert data["total_dbms"] == 2
        assert data["enabled_dbms"] == 1
        assert len(data["databases"]) == 2
        assert data["databases"][0]["name"] == "testdb1"
        # Path should be absolute
        assert os.path.isabs(data["config_path"])

    def test_config_show_missing(self, tmp_dir):
        fake = os.path.join(tmp_dir, "nope.json")
        result = run_cli("config", "show", "--config", fake)
        assert result["ok"] is False

    def test_config_init(self, tmp_dir):
        out_path = os.path.join(tmp_dir, "new_config.json")
        result = run_cli("config", "init", "--output", out_path)
        assert result["ok"] is True
        assert os.path.isfile(out_path)
        # Should be valid JSON
        with open(out_path) as f:
            data = json.load(f)
        assert "databases" in data

    def test_config_init_already_exists(self, sample_config):
        result = run_cli("config", "init", "--output", sample_config)
        assert result["ok"] is False
        assert "exists" in result["error"].lower()

    def test_config_validate(self, sample_config):
        result = run_cli("config", "validate", "--config", sample_config)
        assert result["ok"] is True
        data = result["data"]
        assert data["valid"] is True
        assert data["total_dbms"] == 2

    def test_config_validate_missing(self, tmp_dir):
        fake = os.path.join(tmp_dir, "nope.json")
        result = run_cli("config", "validate", "--config", fake)
        assert result["ok"] is False

    def test_config_validate_invalid_json(self, tmp_dir):
        bad = os.path.join(tmp_dir, "bad.json")
        with open(bad, "w") as f:
            f.write("{invalid json")
        result = run_cli("config", "validate", "--config", bad)
        assert result["ok"] is False

    def test_config_validate_no_databases(self, tmp_dir):
        empty = os.path.join(tmp_dir, "empty.json")
        with open(empty, "w") as f:
            json.dump({"databases": []}, f)
        result = run_cli("config", "validate", "--config", empty)
        assert result["ok"] is False


# ===========================================================================
# 6. mtr / bench — argument parsing (no actual execution)
# ===========================================================================

class TestMtrBenchParsing:
    """Verify that mtr/bench parsers accept all documented arguments."""

    def _parse(self, *argv):
        from rosetta.cli.main import create_parser
        return create_parser().parse_args(list(argv))

    # -- mtr: required args ------------------------------------------------

    def test_mtr_missing_required(self):
        """test without --test and --dbms should fail."""
        with pytest.raises(SystemExit):
            self._parse("test")

    def test_mtr_missing_test(self):
        with pytest.raises(SystemExit):
            self._parse("test", "--dbms", "mysql")

    def test_mtr_missing_dbms(self):
        with pytest.raises(SystemExit):
            self._parse("test", "-t", "test.test")

    # -- test (cross-DBMS consistency): all options ----------------------------

    def test_mtr_basic_args(self):
        args = self._parse(
            "test", "--dbms", "mysql,tdsql", "-t", "test.test",
        )
        assert args.command == "test"
        assert args.test == "test.test"
        assert args.dbms == "mysql,tdsql"

    def test_mtr_baseline(self):
        args = self._parse(
            "test", "--dbms", "mysql,tdsql", "-b", "mysql", "-t", "test.test",
        )
        assert args.baseline == "mysql"

    def test_mtr_database(self):
        args = self._parse(
            "test", "--dbms", "mysql", "-d", "mydb", "-t", "t.test",
        )
        assert args.database == "mydb"

    def test_mtr_output_dir(self):
        args = self._parse(
            "test", "--dbms", "mysql", "-o", "/tmp/out", "-t", "t.test",
        )
        assert args.output_dir == "/tmp/out"

    def test_mtr_output_format(self):
        for fmt in ["text", "html", "all"]:
            args = self._parse(
                "test", "--dbms", "mysql", "-f", fmt, "-t", "t.test",
            )
            assert args.output_format == fmt

    def test_mtr_parse_only(self):
        args = self._parse(
            "test", "--dbms", "mysql", "--parse-only", "-t", "t.test",
        )
        assert args.parse_only is True

    def test_mtr_diff_only(self):
        args = self._parse(
            "test", "--dbms", "mysql", "--diff-only", "-t", "t.test",
        )
        assert args.diff_only is True

    def test_mtr_serve_and_port(self):
        args = self._parse(
            "test", "--dbms", "mysql", "--serve", "-p", "8080", "-t", "t.test",
        )
        assert args.serve is True
        assert args.port == 8080

    def test_mtr_skip_flags(self):
        args = self._parse(
            "test", "--dbms", "mysql",
            "--skip-explain", "--skip-analyze", "--skip-show-create",
            "-t", "t.test",
        )
        assert args.skip_explain is True
        assert args.skip_analyze is True
        assert args.skip_show_create is True

    def test_mtr_defaults(self):
        args = self._parse(
            "test", "--dbms", "mysql", "-t", "t.test",
        )
        assert args.baseline == "tdsql"
        assert args.database == "rosetta_mtr_test"
        assert args.output_dir == "results"
        assert args.output_format == "all"
        assert args.parse_only is False
        assert args.diff_only is False
        assert args.serve is False
        assert args.port == 19527

    def test_mtr_json_after_subcommand(self):
        args = self._parse(
            "test", "--dbms", "mysql", "-j", "-t", "t.test",
        )
        assert args.json is True

    def test_version_flag_without_command(self):
        args = self._parse("--version")
        assert args.version is True
        assert args.command is None

    def test_version_short_flag_without_command(self):
        args = self._parse("-V")
        assert args.version is True
        assert args.command is None

    # -- bench: required args ----------------------------------------------

    def test_bench_missing_dbms(self):
        with pytest.raises(SystemExit):
            self._parse("bench", "--file", "b.json")

    # -- bench: all options ------------------------------------------------

    def test_bench_basic_args(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--file", "bench.json",
        )
        assert args.command == "bench"
        assert args.dbms == "mysql"
        assert args.bench_file == "bench.json"

    def test_bench_mode_serial(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--mode", "SERIAL",
            "--iterations", "10", "--warmup", "3",
            "--file", "b.json",
        )
        assert args.mode == "SERIAL"
        assert args.iterations == 10
        assert args.warmup == 3

    def test_bench_mode_concurrent(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--mode", "CONCURRENT",
            "--concurrency", "8", "--duration", "60", "--ramp-up", "2.5",
            "--file", "b.json",
        )
        assert args.mode == "CONCURRENT"
        assert args.concurrency == 8
        assert args.duration == 60.0
        assert args.ramp_up == 2.5

    def test_bench_database(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "-d", "mydb", "--file", "b.json",
        )
        assert args.database == "mydb"

    def test_bench_output_dir_and_format(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "-o", "/tmp/out", "-f", "html",
            "--file", "b.json",
        )
        assert args.output_dir == "/tmp/out"
        assert args.output_format == "html"

    def test_bench_query_timeout(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--query-timeout", "30",
            "--file", "b.json",
        )
        assert args.query_timeout == 30

    def test_bench_filter(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--bench-filter", "q1,q2",
            "--file", "b.json",
        )
        assert args.bench_filter == "q1,q2"

    def test_bench_repeat(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--repeat", "3",
            "--file", "b.json",
        )
        assert args.repeat == 3

    def test_bench_skip_setup_teardown(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--skip-setup", "--skip-teardown",
            "--file", "b.json",
        )
        assert args.skip_setup is True
        assert args.skip_teardown is True

    def test_bench_no_parallel_dbms(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--no-parallel-dbms",
            "--file", "b.json",
        )
        assert args.parallel_dbms is False

    def test_bench_profile_flags(self):
        # --profile (default on)
        args = self._parse(
            "bench", "--dbms", "mysql", "--profile",
            "--file", "b.json",
        )
        assert args.profile is True

        # --no-profile
        args = self._parse(
            "bench", "--dbms", "mysql", "--no-profile",
            "--file", "b.json",
        )
        assert args.profile is False

    def test_bench_perf_freq(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--perf-freq", "199",
            "--file", "b.json",
        )
        assert args.perf_freq == 199

    def test_bench_template(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--template", "oltp_read_write",
        )
        assert args.template == "oltp_read_write"

    def test_bench_defaults(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "--file", "b.json",
        )
        assert args.mode == "SERIAL"
        assert args.database == "rosetta_bench_test"
        assert args.output_dir == "results"
        assert args.output_format == "all"
        assert args.iterations == 1
        assert args.concurrency == 10
        assert args.duration == 30.0
        assert args.warmup == 0
        assert args.ramp_up == 0.0
        assert args.query_timeout == 5
        assert args.bench_filter is None
        assert args.repeat == 1
        assert args.skip_setup is False
        assert args.skip_teardown is False
        assert args.parallel_dbms is True
        assert args.profile is True
        assert args.perf_freq == 99

    def test_bench_json_after_subcommand(self):
        args = self._parse(
            "bench", "--dbms", "mysql", "-j", "--file", "b.json",
        )
        assert args.json is True


# ===========================================================================
# 7. CommandResult structure
# ===========================================================================

class TestCommandResult:

    def test_success_structure(self):
        from rosetta.cli.result import CommandResult
        r = CommandResult.success("test", {"key": "value"})
        assert r.ok is True
        assert r.command == "test"
        assert r.data == {"key": "value"}
        assert r.exit_code() == 0

        d = r.to_dict()
        assert d["ok"] is True
        assert "timestamp" in d

        j = json.loads(r.to_json())
        assert j["ok"] is True

    def test_failure_structure(self):
        from rosetta.cli.result import CommandResult
        r = CommandResult.failure("something broke", command="test")
        assert r.ok is False
        assert r.error == "something broke"
        assert r.exit_code() == 1
