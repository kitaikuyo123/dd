Load balance test inputs for IDEA SqlCli runs.

## Goal
- Validate node-level rebalance (region skew), not single-region hotspot behavior.

## Core idea
1) Start only RS1 and run setup, so 20 tables (20 regions) are initially placed on one node.
2) Start RS2/RS3/RS4.
3) Run three cross-table activity clients in parallel.
4) Observe whether Master migrates some regions from overloaded node to underloaded nodes.

## Recommended config
In master.properties:
- load.balance.strategy=load_based
- load.balance.enabled=true
- load.balance.interval.ms=15000 (or your preferred value)

To avoid hotspot split interference during this test, temporarily set hotspot thresholds very high.

## Files
- Setup: tests/loadbalance/sql/loadbalance_setup.sql
- Activity:
  - tests/loadbalance/sql/loadbalance_hot_client_1.sql
  - tests/loadbalance/sql/loadbalance_hot_client_2.sql
  - tests/loadbalance/sql/loadbalance_hot_client_3.sql
- Verify: tests/loadbalance/sql/loadbalance_verify.sql
- Cleanup: tests/loadbalance/sql/loadbalance_cleanup.sql
- Optional manual trigger: tests/loadbalance/Trigger-Balance.ps1
- Monitor snapshot helper: tests/loadbalance/Snapshot-Balance.ps1

## Run sequence
1) Run setup input:
- tests/loadbalance/cli-input/loadbalance_setup_input.txt

2) Start RS2/RS3/RS4.

3) Run three clients in parallel:
- tests/loadbalance/cli-input/loadbalance_client_1.txt
- tests/loadbalance/cli-input/loadbalance_client_2.txt
- tests/loadbalance/cli-input/loadbalance_client_3.txt

4) Observe Master log:
- Load balance triggered
- Executing balance action: move ...
- [MIGRATION] ... state=COMPLETED

5) Verify data:
- tests/loadbalance/cli-input/loadbalance_verify_input.txt
(Each table should still report 2 rows.)

6) Cleanup:
- tests/loadbalance/cli-input/loadbalance_cleanup_input.txt

## Notes
- If no action appears, use Trigger-Balance.ps1 for one manual attempt.
- Cooldown and threshold rules may suppress immediate repeated actions.
