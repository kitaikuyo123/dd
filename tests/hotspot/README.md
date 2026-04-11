Hotspot detection test inputs for IDEA SqlCli runs.

Usage summary:
1) In one CLI, run: source tests/hotspot/sql/hotspot_setup.sql
2) For hotspot scenario, start three SqlCli run configs in parallel and redirect input to:
   - tests/hotspot/cli-input/hotspot_client_1.txt
   - tests/hotspot/cli-input/hotspot_client_2.txt
   - tests/hotspot/cli-input/hotspot_client_3.txt
3) For baseline scenario, use:
   - tests/hotspot/cli-input/hotspot_uniform_client_1.txt
   - tests/hotspot/cli-input/hotspot_uniform_client_2.txt
   - tests/hotspot/cli-input/hotspot_uniform_client_3.txt
4) Observe master logs for hotspot detection records.

Rerun notes:
- First run: source tests/hotspot/sql/hotspot_setup.sql
- Rerun with reset: source tests/hotspot/sql/hotspot_cleanup.sql, then source tests/hotspot/sql/hotspot_setup.sql
