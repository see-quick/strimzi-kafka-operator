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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultExporterTest {

    private final ObjectMapper mapper = new ObjectMapper();

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
    }

    @Test
    void testWriteResultToFile(@TempDir Path outputDir) throws Exception {
        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("reconciliationIntervalMs", 74596.0);

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("numberOfTopics", 250);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator",
            "scalability",
            "2026-06-13T02:00:00Z",
            "abc1234",
            parameters,
            metrics
        );

        ResultExporter.writeResult(result, outputDir);

        File outputFile = outputDir.resolve("topic-operator-scalability.json").toFile();
        assertTrue(outputFile.exists());

        TestResult loaded = mapper.readValue(outputFile, TestResult.class);
        assertEquals("topic-operator", loaded.getComponent());
        assertEquals(74596.0, loaded.getMetrics().get("reconciliationIntervalMs"));
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
