Hotspot detection test inputs for IDEA SqlCli runs.

Usage summary:
1) Read-hotspot setup:
- Run `tests/hotspot/cli-input/hotspot_setup_input.txt`
2) For read-hotspot scenario, start three SqlCli run configs in parallel and redirect input to:
   - tests/hotspot/cli-input/hotspot_client_1.txt
   - tests/hotspot/cli-input/hotspot_client_2.txt
   - tests/hotspot/cli-input/hotspot_client_3.txt
3) For read baseline scenario, use:
   - tests/hotspot/cli-input/hotspot_uniform_client_1.txt
   - tests/hotspot/cli-input/hotspot_uniform_client_2.txt
   - tests/hotspot/cli-input/hotspot_uniform_client_3.txt
4) Write-hotspot setup:
- Run `tests/hotspot/cli-input/hotspot_write_setup_input.txt`
5) For write-hotspot scenario, start three SqlCli run configs in parallel and redirect input to:
   - tests/hotspot/cli-input/hotspot_write_client_1.txt
   - tests/hotspot/cli-input/hotspot_write_client_2.txt
   - tests/hotspot/cli-input/hotspot_write_client_3.txt
6) Write-hotspot cleanup:
- Run `tests/hotspot/cli-input/hotspot_write_cleanup_input.txt`
7) Observe master logs for hotspot detection records.

Rerun notes:
- Read first run: source tests/hotspot/sql/hotspot_setup.sql
- Read rerun with reset: source tests/hotspot/sql/hotspot_cleanup.sql, then source tests/hotspot/sql/hotspot_setup.sql
- Write first run: source tests/hotspot/sql/hotspot_write_setup.sql
- Write rerun with reset: source tests/hotspot/sql/hotspot_write_cleanup.sql, then source tests/hotspot/sql/hotspot_write_setup.sql
