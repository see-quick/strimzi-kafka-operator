# NodePoolScalingPerformance

**Description:** Test suite for measuring Kafka node pool scale-up and scale-down performance.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testNodePoolScaleUpDown

**Description:** Measures the wall-clock time to scale a broker node pool from 3 to 5 and back to 3 replicas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled. | Kafka cluster is deployed and ready. |
| 2. | Start Cluster Operator metrics collection. | Metrics collection is running. |
| 3. | Scale broker pool from 3 to 5 replicas via KafkaNodePool CR patch and measure time until all 5 pods are ready. | 5 broker pods are running and ready. |
| 4. | Scale broker pool from 5 to 3 replicas via KafkaNodePool CR patch and measure time until exactly 3 pods are ready. | 3 broker pods are running and ready. |
| 5. | Stop metrics collection and persist performance data. | Performance data written to cluster-operator report directory. |

**Labels:**

* [kafka](labels/kafka.md)

