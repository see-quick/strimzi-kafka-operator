# Cluster Operator Performance Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 5 new performance test suites measuring Cluster Operator operations: rolling updates, node pool scaling, CA certificate renewal, KafkaRebalance (Cruise Control), and KafkaConnect connector scalability -- all designed for KIND clusters.

**Architecture:** Extend the existing performance test framework (`BasePerformanceReporter`, `BaseMetricsCollector`, `PerformanceConstants`) with new constants, a `ClusterOperatorPerformanceReporter`, a `ClusterOperatorMetricsCollector` + scheduler, and 5 test classes. Each test deploys a Kafka cluster, takes pod/cert snapshots, triggers an operation, measures wall-clock time to completion, collects CO metrics, and reports to YAML. Tests use small cluster sizes (3 brokers/controllers) appropriate for KIND.

**Tech Stack:** Java 21, JUnit 5, Fabric8 Kubernetes client, Strimzi API model, kubetest4j, existing systemtest infrastructure.

---

## File Structure

### New files to create:

| File | Purpose |
|------|---------|
| `systemtest/src/main/java/io/strimzi/systemtest/performance/gather/collectors/ClusterOperatorMetricsCollector.java` | Collects CO reconciliation metrics via Prometheus scraper |
| `systemtest/src/main/java/io/strimzi/systemtest/performance/gather/schedulers/ClusterOperatorMetricsCollectionScheduler.java` | Periodically schedules CO metric collection |
| `systemtest/src/main/java/io/strimzi/systemtest/performance/report/ClusterOperatorPerformanceReporter.java` | Resolves directory paths for CO performance logs |
| `systemtest/src/test/java/io/strimzi/systemtest/performance/RollingUpdatePerformance.java` | Test: rolling update timing |
| `systemtest/src/test/java/io/strimzi/systemtest/performance/NodePoolScalingPerformance.java` | Test: scale-up/scale-down timing |
| `systemtest/src/test/java/io/strimzi/systemtest/performance/CaRenewalPerformance.java` | Test: CA cert renewal timing |
| `systemtest/src/test/java/io/strimzi/systemtest/performance/RebalancePerformance.java` | Test: Cruise Control rebalance timing |
| `systemtest/src/test/java/io/strimzi/systemtest/performance/ConnectScalabilityPerformance.java` | Test: connector count scalability |

### Files to modify:

| File | Change |
|------|--------|
| `systemtest/src/main/java/io/strimzi/systemtest/performance/PerformanceConstants.java` | Add CO-specific input/output constants and use case names |
| `systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserType.java` | Add `CLUSTER_OPERATOR` enum value |
| `systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserFactory.java` | Add `CLUSTER_OPERATOR` case to switch |

---

## Task 1: Add CO Performance Constants

**Files:**
- Modify: `systemtest/src/main/java/io/strimzi/systemtest/performance/PerformanceConstants.java`

- [ ] **Step 1: Add Cluster Operator constants to PerformanceConstants.java**

Add a new section between the User Operator section and the `METRICS_HISTORY` constant. Insert the following block after line 116 (after `USER_OPERATOR_OUT_SUCCESSFUL_KAFKA_USERS_CREATED`):

```java
    // --------------------------------------------------------------------------------
    // ----------------------------- CLUSTER OPERATOR ---------------------------------
    // --------------------------------------------------------------------------------
    // Rolling Update
    String CLUSTER_OPERATOR_IN_BROKER_COUNT = "IN: BROKER COUNT";
    String CLUSTER_OPERATOR_IN_CONTROLLER_COUNT = "IN: CONTROLLER COUNT";
    String CLUSTER_OPERATOR_OUT_BROKER_ROLLING_UPDATE_TIME = "OUT: Broker Rolling Update Time (ms)";
    String CLUSTER_OPERATOR_OUT_CONTROLLER_ROLLING_UPDATE_TIME = "OUT: Controller Rolling Update Time (ms)";
    String CLUSTER_OPERATOR_OUT_TOTAL_ROLLING_UPDATE_TIME = "OUT: Total Rolling Update Time (ms)";

    // Node Pool Scaling
    String CLUSTER_OPERATOR_IN_INITIAL_BROKER_COUNT = "IN: INITIAL BROKER COUNT";
    String CLUSTER_OPERATOR_IN_SCALED_BROKER_COUNT = "IN: SCALED BROKER COUNT";
    String CLUSTER_OPERATOR_OUT_SCALE_UP_TIME = "OUT: Scale Up Time (ms)";
    String CLUSTER_OPERATOR_OUT_SCALE_DOWN_TIME = "OUT: Scale Down Time (ms)";

    // CA Certificate Renewal
    String CLUSTER_OPERATOR_IN_CA_TYPE = "IN: CA TYPE";
    String CLUSTER_OPERATOR_OUT_CA_RENEWAL_TIME = "OUT: CA Renewal Time (ms)";
    String CLUSTER_OPERATOR_OUT_CA_CERT_CHANGED = "OUT: CA Certificate Changed";

    // KafkaRebalance
    String CLUSTER_OPERATOR_IN_TOPIC_COUNT = "IN: TOPIC COUNT";
    String CLUSTER_OPERATOR_OUT_PROPOSAL_READY_TIME = "OUT: Proposal Ready Time (ms)";
    String CLUSTER_OPERATOR_OUT_REBALANCE_EXECUTION_TIME = "OUT: Rebalance Execution Time (ms)";
    String CLUSTER_OPERATOR_OUT_TOTAL_REBALANCE_TIME = "OUT: Total Rebalance Time (ms)";

    // KafkaConnect Scalability
    String CLUSTER_OPERATOR_IN_CONNECTOR_COUNT = "IN: CONNECTOR COUNT";
    String CLUSTER_OPERATOR_OUT_CONNECT_RECONCILIATION_TIME = "OUT: Connect Reconciliation Time (ms)";
    String CLUSTER_OPERATOR_OUT_ALL_CONNECTORS_READY_TIME = "OUT: All Connectors Ready Time (ms)";

    // Use cases
    String CLUSTER_OPERATOR_ROLLING_UPDATE_USE_CASE = "rollingUpdateUseCase";
    String CLUSTER_OPERATOR_NODE_POOL_SCALING_USE_CASE = "nodePoolScalingUseCase";
    String CLUSTER_OPERATOR_CA_RENEWAL_USE_CASE = "caRenewalUseCase";
    String CLUSTER_OPERATOR_REBALANCE_USE_CASE = "rebalanceUseCase";
    String CLUSTER_OPERATOR_CONNECT_SCALABILITY_USE_CASE = "connectScalabilityUseCase";

    // Parser
    String CLUSTER_OPERATOR_PARSER = "cluster-operator";
```

- [ ] **Step 2: Verify the file compiles**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS (no compilation errors)

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/main/java/io/strimzi/systemtest/performance/PerformanceConstants.java
git commit -s -m "Add Cluster Operator performance test constants"
```

---

## Task 2: Create ClusterOperatorMetricsCollector

**Files:**
- Create: `systemtest/src/main/java/io/strimzi/systemtest/performance/gather/collectors/ClusterOperatorMetricsCollector.java`

- [ ] **Step 1: Create the collector class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.collectors;

import io.skodjob.kubetest4j.MetricsCollector;
import io.skodjob.kubetest4j.MetricsComponent;

import java.util.List;
import java.util.regex.Pattern;

public class ClusterOperatorMetricsCollector extends BaseMetricsCollector {

    public ClusterOperatorMetricsCollector(MetricsCollector.Builder builder) {
        super(builder);
    }

    public List<Double> getReconciliationsTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsSuccessfulTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_successful_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsFailedTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_failed_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsDurationSecondsSum(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_duration_seconds_sum\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsDurationSecondsMax(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_duration_seconds_max\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsAlreadyEnqueuedTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_already_enqueued_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getResourceCount(String kind) {
        Pattern pattern = Pattern.compile("strimzi_resources\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    @Override
    protected ClusterOperatorMetricsCollector.Builder newBuilder() {
        return new ClusterOperatorMetricsCollector.Builder();
    }

    @Override
    protected ClusterOperatorMetricsCollector.Builder updateBuilder(BaseMetricsCollector.Builder builder) {
        return (ClusterOperatorMetricsCollector.Builder) super.updateBuilder(builder);
    }

    @Override
    public ClusterOperatorMetricsCollector.Builder toBuilder() {
        return this.updateBuilder(this.newBuilder());
    }

    public static class Builder extends BaseMetricsCollector.Builder {
        @Override
        public ClusterOperatorMetricsCollector build() {
            return new ClusterOperatorMetricsCollector(this);
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withNamespaceName(String namespaceName) {
            super.withNamespaceName(namespaceName);
            return this;
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withScraperPodName(String scraperPodName) {
            super.withScraperPodName(scraperPodName);
            return this;
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withComponent(MetricsComponent component) {
            super.withComponent(component);
            return this;
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/main/java/io/strimzi/systemtest/performance/gather/collectors/ClusterOperatorMetricsCollector.java
git commit -s -m "Add ClusterOperatorMetricsCollector for CO performance metrics"
```

---

## Task 3: Create ClusterOperatorMetricsCollectionScheduler

**Files:**
- Create: `systemtest/src/main/java/io/strimzi/systemtest/performance/gather/schedulers/ClusterOperatorMetricsCollectionScheduler.java`

- [ ] **Step 1: Create the scheduler class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.schedulers;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterOperatorMetricsCollectionScheduler extends BaseMetricsCollectionScheduler {

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorMetricsCollectionScheduler.class);
    private final ClusterOperatorMetricsCollector clusterOperatorMetricsCollector;

    public static ClusterOperatorMetricsCollectionScheduler getInstance(ClusterOperatorMetricsCollector collector, String selector) {
        return BaseMetricsCollectionScheduler.getInstance(
            ClusterOperatorMetricsCollectionScheduler.class,
            () -> new ClusterOperatorMetricsCollectionScheduler(collector, selector)
        );
    }

    private ClusterOperatorMetricsCollectionScheduler(ClusterOperatorMetricsCollector clusterOperatorMetricsCollector, String selector) {
        super(selector);
        this.clusterOperatorMetricsCollector = clusterOperatorMetricsCollector;
    }

    @Override
    protected void collectMetrics() {
        LOGGER.debug("Collecting Cluster Operator metrics");
        this.clusterOperatorMetricsCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        LOGGER.debug("Cluster Operator metrics collected.");
    }

    @Override
    public Map<Long, Map<String, List<Double>>> getMetricsStore() {
        return metricsStore;
    }

    @Override
    protected Map<String, List<Double>> buildMetricsMap() {
        Map<String, List<Double>> metrics = new HashMap<>();

        String kafkaKind = Kafka.RESOURCE_KIND;

        metrics.put(PerformanceConstants.RECONCILIATIONS_TOTAL, this.clusterOperatorMetricsCollector.getReconciliationsTotal(kafkaKind));
        metrics.put(PerformanceConstants.RECONCILIATIONS_SUCCESSFUL_TOTAL, this.clusterOperatorMetricsCollector.getReconciliationsSuccessfulTotal(kafkaKind));
        metrics.put(PerformanceConstants.RECONCILIATIONS_FAILED_TOTAL, this.clusterOperatorMetricsCollector.getReconciliationsFailedTotal(kafkaKind));
        metrics.put(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_SUM, this.clusterOperatorMetricsCollector.getReconciliationsDurationSecondsSum(kafkaKind));
        metrics.put(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX, this.clusterOperatorMetricsCollector.getReconciliationsDurationSecondsMax(kafkaKind));

        // JVM and system metrics
        metrics.put(PerformanceConstants.JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL, this.clusterOperatorMetricsCollector.getJvmGcMemoryAllocatedBytesTotal());

        for (Map.Entry<String, Double> entry : this.clusterOperatorMetricsCollector.getJvmMemoryUsedBytes().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.JVM_THREADS_LIVE_THREADS, this.clusterOperatorMetricsCollector.getJvmThreadsLiveThreads());
        metrics.put(PerformanceConstants.SYSTEM_CPU_USAGE, this.clusterOperatorMetricsCollector.getSystemCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_CPU_COUNT, this.clusterOperatorMetricsCollector.getSystemCpuCount());

        for (Map.Entry<String, Double> entry : this.clusterOperatorMetricsCollector.getJvmGcPauseSecondsMax().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        for (Map.Entry<String, Double> entry : this.clusterOperatorMetricsCollector.getJvmMemoryMaxBytes().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.PROCESS_CPU_USAGE, this.clusterOperatorMetricsCollector.getProcessCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M, this.clusterOperatorMetricsCollector.getSystemLoadAverage1m());

        // Derived metrics
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT,
            this.calculateLoadAveragePerCore(
                metrics.get(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M),
                metrics.get(PerformanceConstants.SYSTEM_CPU_COUNT)
            ));

        Map<String, Double> jvmMemoryUsedBytes = this.clusterOperatorMetricsCollector.getJvmMemoryUsedBytes();
        double totalJvmMemoryUsedBytesInMB = jvmMemoryUsedBytes.values().stream()
            .mapToDouble(Double::doubleValue)
            .sum() / 1_000_000;
        metrics.put(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL, Collections.singletonList(totalJvmMemoryUsedBytesInMB));

        for (Map.Entry<String, List<Double>> entry : metrics.entrySet()) {
            if (!entry.getKey().startsWith("strimzi_")) {
                LOGGER.debug("Metric: {} - Values: {}", entry.getKey(), entry.getValue());
            }
        }

        return metrics;
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/main/java/io/strimzi/systemtest/performance/gather/schedulers/ClusterOperatorMetricsCollectionScheduler.java
git commit -s -m "Add ClusterOperatorMetricsCollectionScheduler for periodic CO metrics"
```

---

## Task 4: Create ClusterOperatorPerformanceReporter

**Files:**
- Create: `systemtest/src/main/java/io/strimzi/systemtest/performance/report/ClusterOperatorPerformanceReporter.java`

- [ ] **Step 1: Create the reporter class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Map;

public class ClusterOperatorPerformanceReporter extends BasePerformanceReporter {

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorPerformanceReporter.class);

    @Override
    protected Path resolveComponentUseCasePathDir(Path performanceLogDir, String useCaseName, Map<String, Object> performanceAttributes) {
        final String brokerCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, "").toString();
        final String controllerCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, "").toString();

        StringBuilder dirPathBuilder = new StringBuilder();
        dirPathBuilder.append(useCaseName);

        if (!brokerCount.isEmpty()) {
            dirPathBuilder.append("/brokers-").append(brokerCount);
        }
        if (!controllerCount.isEmpty()) {
            dirPathBuilder.append("-controllers-").append(controllerCount);
        }

        // Append use-case specific suffixes
        String connectorCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CONNECTOR_COUNT, "").toString();
        if (!connectorCount.isEmpty()) {
            dirPathBuilder.append("-connectors-").append(connectorCount);
        }

        String caType = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CA_TYPE, "").toString();
        if (!caType.isEmpty()) {
            dirPathBuilder.append("-ca-").append(caType);
        }

        String topicCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_TOPIC_COUNT, "").toString();
        if (!topicCount.isEmpty()) {
            dirPathBuilder.append("-topics-").append(topicCount);
        }

        String initialBrokers = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_INITIAL_BROKER_COUNT, "").toString();
        String scaledBrokers = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_SCALED_BROKER_COUNT, "").toString();
        if (!initialBrokers.isEmpty() && !scaledBrokers.isEmpty()) {
            dirPathBuilder.append("-scale-").append(initialBrokers).append("-to-").append(scaledBrokers);
        }

        final Path clusterOperatorUseCasePathDir = performanceLogDir.resolve(dirPathBuilder.toString());

        LOGGER.info("Resolved CO performance log directory: {} for use case '{}'", clusterOperatorUseCasePathDir, useCaseName);

        return clusterOperatorUseCasePathDir;
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/main/java/io/strimzi/systemtest/performance/report/ClusterOperatorPerformanceReporter.java
git commit -s -m "Add ClusterOperatorPerformanceReporter for CO performance logs"
```

---

## Task 5: Update ParserType and ParserFactory (Optional parser support)

**Files:**
- Modify: `systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserType.java`
- Modify: `systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserFactory.java`

- [ ] **Step 1: Add CLUSTER_OPERATOR to ParserType enum**

In `ParserType.java`, add a new enum value after `USER_OPERATOR`:

```java
    TOPIC_OPERATOR("topic-operator"),
    USER_OPERATOR("user-operator"),
    CLUSTER_OPERATOR("cluster-operator");
```

- [ ] **Step 2: Add CLUSTER_OPERATOR case to ParserFactory**

In `ParserFactory.java`, update the switch statement. Since we do not have a dedicated parser yet (the existing `TopicOperatorMetricsParser` and `UserOperatorMetricsParser` are very specific), we will reuse the generic table-building logic. For now, add the case that throws a descriptive error:

```java
    public static BasePerformanceMetricsParser createParser(ParserType type) {
        return switch (type) {
            case TOPIC_OPERATOR -> new TopicOperatorMetricsParser();
            case USER_OPERATOR -> new UserOperatorMetricsParser();
            case CLUSTER_OPERATOR -> throw new UnsupportedOperationException("Cluster Operator parser not yet implemented - use raw YAML output");
        };
    }
```

- [ ] **Step 3: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserType.java systemtest/src/main/java/io/strimzi/systemtest/performance/report/parser/ParserFactory.java
git commit -s -m "Add CLUSTER_OPERATOR parser type placeholder"
```

---

## Task 6: Rolling Update Performance Test

**Files:**
- Create: `systemtest/src/test/java/io/strimzi/systemtest/performance/RollingUpdatePerformance.java`

- [ ] **Step 1: Create the test class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;

@SuiteDoc(
    description = @Desc("Test suite for measuring Kafka cluster rolling update performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class RollingUpdatePerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdatePerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures the wall-clock time for a manual rolling update of brokers and controllers in a 3-broker, 3-controller Kafka cluster."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Take pod snapshots for brokers and controllers.", expected = "Pod snapshots captured."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Annotate broker StrimziPodSet with manual-rolling-update=true and measure time until all broker pods roll.", expected = "All broker pods have been recreated."),
            @Step(value = "Annotate controller StrimziPodSet with manual-rolling-update=true and measure time until all controller pods roll.", expected = "All controller pods have been recreated."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testManualRollingUpdate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Snapshot pods before rolling update
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        // --- Roll brokers ---
        long brokerRollStart = System.currentTimeMillis();
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(),
            Collections.singletonMap(ResourceAnnotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerCount, brokerPods);
        long brokerRollTime = System.currentTimeMillis() - brokerRollStart;

        // --- Roll controllers ---
        long controllerRollStart = System.currentTimeMillis();
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(),
            Collections.singletonMap(ResourceAnnotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerCount, controllerPods);
        long controllerRollTime = System.currentTimeMillis() - controllerRollStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Broker rolling update took {} ms, controller rolling update took {} ms", brokerRollTime, controllerRollTime);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_BROKER_ROLLING_UPDATE_TIME, brokerRollTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CONTROLLER_ROLLING_UPDATE_TIME, controllerRollTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_TOTAL_ROLLING_UPDATE_TIME, brokerRollTime + controllerRollTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_ROLLING_UPDATE_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/test/java/io/strimzi/systemtest/performance/RollingUpdatePerformance.java
git commit -s -m "Add RollingUpdatePerformance test measuring manual rolling update time"
```

---

## Task 7: Node Pool Scaling Performance Test

**Files:**
- Create: `systemtest/src/test/java/io/strimzi/systemtest/performance/NodePoolScalingPerformance.java`

- [ ] **Step 1: Create the test class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring Kafka node pool scale-up and scale-down performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class NodePoolScalingPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(NodePoolScalingPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures the wall-clock time to scale a broker node pool from 3 to 5 and back to 3 replicas."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Scale broker pool from 3 to 5 replicas via KafkaNodePool CR patch and measure time until all 5 pods are ready.", expected = "5 broker pods are running and ready."),
            @Step(value = "Scale broker pool from 5 to 3 replicas via KafkaNodePool CR patch and measure time until exactly 3 pods are ready.", expected = "3 broker pods are running and ready."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testNodePoolScaleUpDown() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int initialBrokers = 3;
        final int scaledBrokers = 5;
        final int controllerCount = 3;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialBrokers).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), initialBrokers)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // --- Scale up: 3 -> 5 ---
        long scaleUpStart = System.currentTimeMillis();
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), scaledBrokers);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaledBrokers);
        long scaleUpTime = System.currentTimeMillis() - scaleUpStart;

        LOGGER.info("Scale up from {} to {} took {} ms", initialBrokers, scaledBrokers, scaleUpTime);

        // --- Scale down: 5 -> 3 ---
        long scaleDownStart = System.currentTimeMillis();
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), initialBrokers);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialBrokers);
        long scaleDownTime = System.currentTimeMillis() - scaleDownStart;

        LOGGER.info("Scale down from {} to {} took {} ms", scaledBrokers, initialBrokers, scaleDownTime);

        metricsScheduler.stopCollecting();

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, initialBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_INITIAL_BROKER_COUNT, initialBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_SCALED_BROKER_COUNT, scaledBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_SCALE_UP_TIME, scaleUpTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_SCALE_DOWN_TIME, scaleDownTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_NODE_POOL_SCALING_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/test/java/io/strimzi/systemtest/performance/NodePoolScalingPerformance.java
git commit -s -m "Add NodePoolScalingPerformance test measuring scale-up and scale-down time"
```

---

## Task 8: CA Certificate Renewal Performance Test

**Files:**
- Create: `systemtest/src/test/java/io/strimzi/systemtest/performance/CaRenewalPerformance.java`

- [ ] **Step 1: Create the test class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;

@SuiteDoc(
    description = @Desc("Test suite for measuring CA certificate renewal performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class CaRenewalPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CaRenewalPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures end-to-end time from force-renew annotation on Cluster CA to all broker and controller pods being restarted with the new certificate."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Capture current Cluster CA certificate value and take pod snapshots.", expected = "Snapshots captured."),
            @Step(value = "Annotate Cluster CA Secret with force-renew=true.", expected = "Secret annotated."),
            @Step(value = "Wait for CA certificate to change and all broker and controller pods to roll.", expected = "New certificate is in the Secret and all pods have restarted."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testClusterCaRenewal() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Capture current cert and pod state
        String clusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName());
        String originalCaCert = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(clusterCaSecretName)
            .get().getData().get("ca.crt");

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        // Annotate Cluster CA secret to force renewal
        long renewalStart = System.currentTimeMillis();
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), clusterCaSecretName,
            ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        LOGGER.info("Waiting for Cluster CA certificate to change and pods to roll");

        // Wait for the cert to actually change
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), originalCaCert, clusterCaSecretName, "ca.crt");

        // Wait for both brokers and controllers to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerCount, brokerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerCount, controllerPods);

        long renewalTime = System.currentTimeMillis() - renewalStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Cluster CA renewal (cert change + full rolling restart) took {} ms", renewalTime);

        // Verify cert actually changed
        String newCaCert = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(clusterCaSecretName)
            .get().getData().get("ca.crt");
        boolean certChanged = !originalCaCert.equals(newCaCert);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CA_TYPE, "cluster-ca");
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CA_RENEWAL_TIME, renewalTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CA_CERT_CHANGED, certChanged);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_CA_RENEWAL_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add systemtest/src/test/java/io/strimzi/systemtest/performance/CaRenewalPerformance.java
git commit -s -m "Add CaRenewalPerformance test measuring certificate renewal time"
```

---

## Task 9: KafkaRebalance Performance Test

**Files:**
- Create: `systemtest/src/test/java/io/strimzi/systemtest/performance/RebalancePerformance.java`

- [ ] **Step 1: Create the test class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;

@SuiteDoc(
    description = @Desc("Test suite for measuring KafkaRebalance (Cruise Control) performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class RebalancePerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RebalancePerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures end-to-end rebalance time: from KafkaRebalance CR creation through ProposalReady to Ready state."),
        steps = {
            @Step(value = "Deploy Kafka cluster with Cruise Control and 3 brokers, create topics to give CC data to work with.", expected = "Kafka cluster with Cruise Control deployed, topics created."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Create KafkaRebalance CR and record start time.", expected = "KafkaRebalance CR created."),
            @Step(value = "Wait for ProposalReady state and record proposal time.", expected = "Proposal generated."),
            @Step(value = "Approve the rebalance and wait for Ready state, recording execution time.", expected = "Rebalance completed."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testRebalancePerformance() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;
        final int topicCount = 50;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.cruiseControlMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        // Create topics to give Cruise Control some partitions to work with
        for (int i = 0; i < topicCount; i++) {
            KubeResourceManager.get().createResourceWithoutWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), "perf-rebalance-topic-" + i, testStorage.getClusterName(), 3, 3)
                    .build()
            );
        }

        // Wait a bit for CC to gather metrics about the new topics
        LOGGER.info("Waiting for Cruise Control to gather enough metrics samples");
        try {
            Thread.sleep(60_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Create KafkaRebalance and measure phases
        long totalStart = System.currentTimeMillis();

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build()
        );

        // Phase 1: Wait for ProposalReady
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(
            testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        long proposalReadyTime = System.currentTimeMillis() - totalStart;
        LOGGER.info("Proposal ready in {} ms", proposalReadyTime);

        // Phase 2: Approve and wait for Ready (execution)
        long executionStart = System.currentTimeMillis();
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
        long executionTime = System.currentTimeMillis() - executionStart;

        long totalRebalanceTime = System.currentTimeMillis() - totalStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Rebalance total: {} ms (proposal: {} ms, execution: {} ms)", totalRebalanceTime, proposalReadyTime, executionTime);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_TOPIC_COUNT, topicCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_PROPOSAL_READY_TIME, proposalReadyTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_REBALANCE_EXECUTION_TIME, executionTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_TOTAL_REBALANCE_TIME, totalRebalanceTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_REBALANCE_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }
}
```

- [ ] **Step 2: Verify the `KafkaTopicTemplates.topic()` method exists**

Run:
```bash
grep -n "public static.*topic(" /Users/morsak/Documents/Work/strimzi-kafka-operator/systemtest/src/main/java/io/strimzi/systemtest/templates/crd/KafkaTopicTemplates.java | head -5
```

If the method signature differs (e.g., different parameter order or names), update the `KafkaTopicTemplates.topic(...)` call in step 1 to match the actual signature. Common signature variants:
- `topic(String namespaceName, String topicName, String kafkaClusterName, int partitions, int replicas)`
- `topic(String namespaceName, String topicName, String kafkaClusterName)`

- [ ] **Step 3: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add systemtest/src/test/java/io/strimzi/systemtest/performance/RebalancePerformance.java
git commit -s -m "Add RebalancePerformance test measuring Cruise Control rebalance time"
```

---

## Task 10: KafkaConnect Connector Scalability Performance Test

**Files:**
- Create: `systemtest/src/test/java/io/strimzi/systemtest/performance/ConnectScalabilityPerformance.java`

- [ ] **Step 1: Create the test class**

```java
/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring KafkaConnect connector scalability."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class ConnectScalabilityPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectScalabilityPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures how long it takes to deploy and reconcile increasing numbers of KafkaConnector CRs (10, 25, 50)."),
        steps = {
            @Step(value = "Deploy Kafka cluster and KafkaConnect with file sink plugin.", expected = "Kafka and KafkaConnect clusters are deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "For each connector count (10, 25, 50): create N KafkaConnector CRs and measure time until all reach Ready state.", expected = "All connectors are Ready. Time recorded for each batch."),
            @Step(value = "Clean up connectors between iterations.", expected = "All connectors deleted."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testConnectorScalability() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;
        final List<Integer> connectorCounts = List.of(10, 25, 50);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        // Deploy KafkaConnect with the file sink plugin and annotations for connectors
        KubeResourceManager.get().createResourceWithWait(
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .build()
        );

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        connectorCounts.forEach(numberOfConnectors -> {
            long allConnectorsReadyTime = 0;
            try {
                // Create N connectors without waiting
                long createStart = System.currentTimeMillis();
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    KubeResourceManager.get().createResourceWithoutWait(
                        KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), connectorName, testStorage.getClusterName(), 1)
                            .editSpec()
                                .addToConfig("file", "/opt/kafka/LICENSE")
                                .addToConfig("topic", "perf-topic-" + i)
                            .endSpec()
                            .build()
                    );
                }

                // Wait for all connectors to be ready
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    var connector = CrdClients.kafkaConnectorClient()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(connectorName)
                        .get();
                    KubeResourceManager.get().waitResourceCondition(connector, ResourceConditions.resourceIsReady());
                }

                allConnectorsReadyTime = System.currentTimeMillis() - createStart;
                LOGGER.info("{} connectors reached Ready state in {} ms", numberOfConnectors, allConnectorsReadyTime);
            } finally {
                // Cleanup connectors
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    CrdClients.kafkaConnectorClient()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(connectorName)
                        .delete();
                }

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONNECTOR_COUNT, numberOfConnectors);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_ALL_CONNECTORS_READY_TIME, allConnectorsReadyTime);

                try {
                    this.reporter.logPerformanceData(testStorage, performanceAttributes,
                        REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_CONNECT_SCALABILITY_USE_CASE,
                        TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        metricsScheduler.stopCollecting();
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();
    }
}
```

- [ ] **Step 2: Verify `KafkaConnectTemplates.kafkaConnectWithFilePlugin` signature**

Run:
```bash
grep -A5 "public static KafkaConnectBuilder kafkaConnectWithFilePlugin" /Users/morsak/Documents/Work/strimzi-kafka-operator/systemtest/src/main/java/io/strimzi/systemtest/templates/crd/KafkaConnectTemplates.java
```

The method `kafkaConnectWithFilePlugin(String namespaceName, String kafkaConnectClusterName, String kafkaClusterName, int replicas)` takes 4 parameters. If the actual signature differs, update the call in step 1.

- [ ] **Step 3: Verify `CrdClients.kafkaConnectorClient()` exists**

Run:
```bash
grep "kafkaConnectorClient\|kafkaConnectorOperation" /Users/morsak/Documents/Work/strimzi-kafka-operator/systemtest/src/main/java/io/strimzi/systemtest/resources/CrdClients.java | head -5
```

If the method name differs, update the reference in the test class.

- [ ] **Step 4: Verify `ResourceConditions.resourceIsReady()` exists**

Run:
```bash
grep "resourceIsReady" /Users/morsak/Documents/Work/strimzi-kafka-operator/systemtest/src/main/java/io/strimzi/systemtest/resources/ResourceConditions.java | head -5
```

If the method does not exist, use the pattern from existing tests (e.g., `waitForKafkaConnectorReady` from `KafkaConnectorUtils`).

- [ ] **Step 5: Verify compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests -q 2>&1 | tail -5
```
Expected: BUILD SUCCESS. If compilation fails, fix import issues based on the method verifications in steps 2-4.

- [ ] **Step 6: Commit**

```bash
git add systemtest/src/test/java/io/strimzi/systemtest/performance/ConnectScalabilityPerformance.java
git commit -s -m "Add ConnectScalabilityPerformance test measuring connector scalability"
```

---

## Task 11: Verify Full Compilation and Checkstyle

- [ ] **Step 1: Run full systemtest module compilation**

Run:
```bash
mvn compile -pl systemtest -am -DskipTests 2>&1 | tail -20
```
Expected: BUILD SUCCESS

- [ ] **Step 2: Run checkstyle on the new files**

Run:
```bash
mvn checkstyle:check@validate -pl systemtest 2>&1 | tail -30
```
Expected: No checkstyle violations. If there are violations, fix them (typically: line length, import ordering, missing Javadoc on public methods).

- [ ] **Step 3: Fix any issues found in step 2**

Common checkstyle fixes needed:
- Line length > 150 chars: break long lines
- Unused imports: remove them
- Import order: organize alphabetically within groups (java, javax, io.strimzi, etc.)

- [ ] **Step 4: Commit fixes if any**

```bash
git add -A
git commit -s -m "Fix checkstyle issues in new performance tests"
```
