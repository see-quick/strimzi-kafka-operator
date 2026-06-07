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
