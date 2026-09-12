/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultExporterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testDeriveComponentMetrics() {
        Map<String, List<Double>> componentMetrics = new LinkedHashMap<>();
        componentMetrics.put("jvm_memory_used_megabytes_total.txt", List.of(150.0, 200.0, 175.0));
        componentMetrics.put("process_cpu_usage.txt", List.of(0.10, 0.30, 0.20));
        componentMetrics.put("jvm_threads_live_threads.txt", List.of(40.0, 45.0));
        componentMetrics.put("action=end of minor GC,cause=Allocation Failure,gc=Copy.txt", List.of(0.05, 0.02));
        componentMetrics.put("action=end of major GC,cause=Allocation Failure,gc=MarkSweepCompact.txt", List.of(0.105));
        componentMetrics.put("strimzi_reconciliations_duration_seconds_max.txt", List.of(12.5, 91.55));
        componentMetrics.put("strimzi_reconciliations_duration_seconds_sum.txt", List.of(10.0, 30.0));
        componentMetrics.put("strimzi_reconciliations_total.txt", List.of(4.0, 12.0));
        componentMetrics.put("strimzi_reconciliations_failed_total.txt", List.of(0.0, 1.0));

        Map<String, Double> derived = ResultExporter.deriveComponentMetrics(componentMetrics);

        assertEquals(200.0, derived.get("jvmMemoryUsedMaxMb"));
        assertEquals(175.0, derived.get("jvmMemoryUsedAvgMb"));
        assertEquals(0.30, derived.get("processCpuUsageMax"));
        assertEquals(0.20, derived.get("processCpuUsageAvg"));
        assertEquals(45.0, derived.get("jvmThreadsLiveMax"));
        assertEquals(0.105, derived.get("jvmGcPauseMaxSeconds"));
        assertEquals(91.55, derived.get("reconciliationDurationMaxSeconds"));
        // cumulative counters: latest sum 30.0 over latest count 12.0
        assertEquals(2.5, derived.get("reconciliationDurationAvgSeconds"));
        assertEquals(1.0, derived.get("reconciliationsFailedTotal"));
    }

    @Test
    void testDeriveComponentMetricsSkipsMissingAndEmptyFiles() {
        Map<String, List<Double>> componentMetrics = new LinkedHashMap<>();
        componentMetrics.put("jvm_memory_used_megabytes_total.txt", List.of());
        componentMetrics.put("strimzi_reconciliations_duration_seconds_sum.txt", List.of(10.0));
        // no strimzi_reconciliations_total.txt, so no average can be derived

        Map<String, Double> derived = ResultExporter.deriveComponentMetrics(componentMetrics);

        assertTrue(derived.isEmpty());
    }

    @Test
    void testInformationalMetricClassification() {
        assertTrue(ResultExporter.isInformationalMetric("jvmMemoryUsedMaxMb"));
        assertTrue(ResultExporter.isInformationalMetric("jvmGcPauseMaxSeconds"));
        assertTrue(ResultExporter.isInformationalMetric("processCpuUsageAvg"));
        assertTrue(ResultExporter.isInformationalMetric("reconciliationDurationMaxSeconds"));
        assertTrue(ResultExporter.isInformationalMetric("reconciliationsFailedTotal"));

        assertFalse(ResultExporter.isInformationalMetric("reconciliationIntervalMs"));
        assertFalse(ResultExporter.isInformationalMetric("scaleUpTimeMs"));
        assertFalse(ResultExporter.isInformationalMetric("p99LatencyMs"));
        assertFalse(ResultExporter.isInformationalMetric("caRenewalTimeMs"));
    }

    @Test
    void testExportSingleTestResult(@TempDir Path outputDir) throws Exception {
        Map<String, String> testMetrics = new LinkedHashMap<>();
        testMetrics.put("IN: NUMBER OF TOPICS", "250");
        testMetrics.put("IN: MAX BATCH SIZE (ms)", "100");
        testMetrics.put("OUT: Reconciliation interval (ms)", "74596");

        TestResult result = ResultExporter.convertMetrics(
            testMetrics,
            "topic-operator",
            "scalabilityUseCase",
            "2026-06-13T02:00:00Z",
            "abc1234"
        );

        assertEquals("topic-operator", result.getComponent());
        assertEquals("scalabilityUseCase", result.getUseCase());
        assertEquals(1, result.getMetrics().size());
        assertEquals(74596.0, result.getMetrics().get("reconciliationIntervalMs"));
        assertEquals(2, result.getParameters().size());
        assertTrue(result.getTestName().contains("numberOfTopics=250"));
    }

    @Test
    void testWriteResultWithParameterSuffix(@TempDir Path outputDir) throws Exception {
        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("reconciliationIntervalMs", 74596.0);

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("numberOfTopics", 250);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance (numberOfTopics=250)",
            "topic-operator",
            "scalability",
            "2026-06-13T02:00:00Z",
            "abc1234",
            parameters,
            metrics
        );

        ResultExporter.writeResult(result, outputDir);

        File outputFile = outputDir.resolve("topic-operator-scalability-numberOfTopics-250.json").toFile();
        assertTrue(outputFile.exists());

        TestResult loaded = mapper.readValue(outputFile, TestResult.class);
        assertEquals("topic-operator", loaded.getComponent());
        assertEquals(74596.0, loaded.getMetrics().get("reconciliationIntervalMs"));
    }

    @Test
    void testWriteResultWithoutDistinguishingParam(@TempDir Path outputDir) throws Exception {
        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("caRenewalTimeMs", 165058.0);

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("brokerCount", 3);

        TestResult result = new TestResult(
            "ClusterOperatorCaRenewalPerformance",
            "cluster-operator",
            "caRenewal",
            "2026-06-13T02:00:00Z",
            "abc1234",
            parameters,
            metrics
        );

        ResultExporter.writeResult(result, outputDir);

        File outputFile = outputDir.resolve("cluster-operator-caRenewal.json").toFile();
        assertTrue(outputFile.exists());
    }

    @Test
    void testWriteMetadata(@TempDir Path outputDir) throws Exception {
        ResultMetadata metadata = new ResultMetadata(
            "abc1234", "main", "2026-06-13T02:00:00Z",
            "1.30.2", "3.9.0", "0.45.0-SNAPSHOT"
        );

        ResultExporter.writeMetadata(metadata, outputDir);

        File metadataFile = outputDir.resolve("metadata.json").toFile();
        assertTrue(metadataFile.exists());

        ResultMetadata loaded = mapper.readValue(metadataFile, ResultMetadata.class);
        assertEquals("abc1234", loaded.getCommitSha());
        assertEquals("main", loaded.getBranch());
    }

    @Test
    void testMetricKeyNormalization() {
        assertEquals("reconciliationIntervalMs", ResultExporter.normalizeMetricKey("OUT: Reconciliation interval (ms)"));
        assertEquals("creationTime", ResultExporter.normalizeMetricKey("OUT: Creation Time"));
        assertEquals("brokerRollingUpdateTimeMs", ResultExporter.normalizeMetricKey("OUT: Broker Rolling Update Time (ms)"));
    }

    @Test
    void testParameterKeyNormalization() {
        assertEquals("numberOfTopics", ResultExporter.normalizeMetricKey("IN: NUMBER OF TOPICS"));
        assertEquals("maxBatchSizeMs", ResultExporter.normalizeMetricKey("IN: MAX BATCH SIZE (ms)"));
    }
}
