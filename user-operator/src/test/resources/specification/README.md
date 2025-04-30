#  UserOperatorModel — Quint Specification

This Quint specification models the behavior of a Kafka User Operator, including user creation, update, deletion, secrets, quotas, ACLs, and a reconciliation loop.

## Run Simulation (State Exploration)

Run this to simulate the model and check all safety invariants (e.g., secrets must exist for Ready users, quotas are valid, etc.):

```shell
quint run UserOperatorModel.qnt --invariant=AllInvariantsHold
```

This will execute the model nondeterministically and stop after a bounded number of events (default: 1000), checking all assertions.

> [!WARNING]  
> for temporal verification, reduce maxProcessedEvents to ≤ 10.

## Safety Property Verification (with Apalache)

To formally verify that the invariants always hold:

```shell
quint verify UserOperator3Users.qnt --invariant=AllInvariantsHold
```

Use a reduced version like UserOperator3Users.qnt to keep the state space small.

## Temporal Property Verification (with TLC)

To verify temporal properties like eventual reconciliation:

```shell
sh ../tlc/check_with_tlc.sh --file UserOperatorModel.qnt --temporal EventuallyStableReconciliationProperty
```

TLC script available at:
👉 https://github.com/informalsystems/quint/blob/main/tlc/check_with_tlc.sh

### Notes
- Bounded trace length via parameters.maxProcessedEvents prevents infinite runs.
- parameters.aclsEnabled randomly enables/disables ACLs during simulation.
- Each step non-deterministically chooses to generate or process an event.