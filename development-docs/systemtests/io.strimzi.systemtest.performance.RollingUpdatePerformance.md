# RollingUpdatePerformance

**Description:** Test suite for measuring Kafka cluster rolling update performance.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testManualRollingUpdate

**Description:** Measures the wall-clock time for a manual rolling update of brokers and controllers in a 3-broker, 3-controller Kafka cluster.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled. | Kafka cluster is deployed and ready. |
| 2. | Take pod snapshots for brokers and controllers. | Pod snapshots captured. |
| 3. | Start Cluster Operator metrics collection. | Metrics collection is running. |
| 4. | Annotate broker StrimziPodSet with manual-rolling-update=true and measure time until all broker pods roll. | All broker pods have been recreated. |
| 5. | Annotate controller StrimziPodSet with manual-rolling-update=true and measure time until all controller pods roll. | All controller pods have been recreated. |
| 6. | Stop metrics collection and persist performance data. | Performance data written to cluster-operator report directory. |

**Labels:**

* [kafka](labels/kafka.md)

