# RebalancePerformance

**Description:** Test suite for measuring KafkaRebalance (Cruise Control) performance.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testRebalancePerformance

**Description:** Measures end-to-end rebalance time: from KafkaRebalance CR creation through ProposalReady to Ready state.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with Cruise Control and 3 brokers, create topics to give CC data to work with. | Kafka cluster with Cruise Control deployed, topics created. |
| 2. | Start Cluster Operator metrics collection. | Metrics collection is running. |
| 3. | Create KafkaRebalance CR and record start time. | KafkaRebalance CR created. |
| 4. | Wait for ProposalReady state and record proposal time. | Proposal generated. |
| 5. | Approve the rebalance and wait for Ready state, recording execution time. | Rebalance completed. |
| 6. | Stop metrics collection and persist performance data. | Performance data written to cluster-operator report directory. |

**Labels:**

* [kafka](labels/kafka.md)

