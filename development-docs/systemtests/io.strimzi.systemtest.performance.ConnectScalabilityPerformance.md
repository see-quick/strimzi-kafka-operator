# ConnectScalabilityPerformance

**Description:** Test suite for measuring KafkaConnect connector scalability.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testConnectorScalability

**Description:** Measures how long it takes to deploy and reconcile increasing numbers of KafkaConnector CRs (10, 25, 50).

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster and KafkaConnect with echo-sink plugin. | Kafka and KafkaConnect clusters are deployed and ready. |
| 2. | Start Cluster Operator metrics collection. | Metrics collection is running. |
| 3. | For each connector count (10, 25, 50): create N KafkaConnector CRs and measure time until all reach Ready state. | All connectors are Ready. Time recorded for each batch. |
| 4. | Clean up connectors between iterations. | All connectors deleted. |
| 5. | Stop metrics collection and persist performance data. | Performance data written to cluster-operator report directory. |

**Labels:**

* [kafka](labels/kafka.md)

