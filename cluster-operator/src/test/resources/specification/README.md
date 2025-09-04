# ClusterOperatorModel — Quint Specification

This Quint specification models the behavior of the Strimzi Cluster Operator, including Kafka cluster management, node pool scaling, rolling updates, certificate rotation, and metadata version management.

## Generate Test Traces

To generate test traces for model-based testing, run:

```shell
quint run ClusterOperatorModel.qnt --invariant=AllInvariantsHold --max-steps=20 --out-itf=traces/test_case0.json
```

Generate multiple test cases with different parameters:

```shell
# Generate 10 different test traces
for i in {0..9}; do
  quint run ClusterOperatorModel.qnt \
    --invariant=AllInvariantsHold \
    --max-steps=15 \
    --out-itf=traces/test_case${i}.json \
    --seed=$i
done
```

## Run Simulation (State Exploration)

Run this to simulate the model and check all safety invariants:

```shell
quint run ClusterOperatorModel.qnt --invariant=AllInvariantsHold
```

Key invariants verified:
- **ClusterIdConsistency**: All pods report the same cluster ID
- **MetadataVersionMonotonicity**: Metadata versions only increase
- **MinimumReadyReplicas**: Maintain quorum during rolling updates
- **CertificateValidityOverlap**: Certificates remain valid during rotation
- **NodeIdUniqueness**: No duplicate node IDs across node pools
- **AutoRebalanceConsistency**: Rebalance state matches cluster topology

## Safety Property Verification (with Apalache)

To formally verify that the invariants always hold:

```shell
quint verify ClusterOperatorModel.qnt --invariant=AllInvariantsHold
```

## Temporal Property Verification (with TLC)

To verify temporal properties like eventual cluster readiness:

```shell
sh ../tlc/check_with_tlc.sh ClusterOperatorModel.qnt --temporal EventualReadiness
```

## Key Model Features

- **Pod Lifecycle**: Creation, readiness, termination, failure states
- **Rolling Updates**: Safe pod replacement maintaining minimum replicas
- **Certificate Rotation**: Cluster CA and clients CA with validity overlap
- **Node Pool Scaling**: Scale up/down with node ID tracking
- **Metadata Versioning**: Monotonic version upgrades across brokers
- **Auto-Rebalancing**: Track scaled brokers for rebalancing coordination

## Notes

- Bounded trace length via `parameters.maxProcessedEvents` prevents infinite runs
- `parameters.enableRollingUpdates`, `enableCertRotation`, `enableAutoRebalance` control feature testing
- Each step non-deterministically chooses cluster operations or event processing
- Model focuses on critical safety properties that could cause cluster instability