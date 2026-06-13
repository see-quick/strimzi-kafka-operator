# CaRenewalPerformance

**Description:** Test suite for measuring CA certificate renewal performance.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with default configuration. | Cluster Operator is deployed and running. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testClusterCaRenewal

**Description:** Measures end-to-end time from force-renew annotation on Cluster CA to all broker and controller pods being restarted with the new certificate.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled. | Kafka cluster is deployed and ready. |
| 2. | Start Cluster Operator metrics collection. | Metrics collection is running. |
| 3. | Capture current Cluster CA certificate value and take pod snapshots. | Snapshots captured. |
| 4. | Annotate Cluster CA Secret with force-renew=true. | Secret annotated. |
| 5. | Wait for CA certificate to change and all broker and controller pods to roll. | New certificate is in the Secret and all pods have restarted. |
| 6. | Stop metrics collection and persist performance data. | Performance data written to cluster-operator report directory. |

**Labels:**

* [kafka](labels/kafka.md)

