/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PerformanceBaselineComparatorTest {

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Test
    void testFirstRunInitializesBaseline(@TempDir Path repoDir) throws Exception {
        Path resultsDir = repoDir.resolve("results").resolve("2026-06-13");
        Files.createDirectories(resultsDir);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator", "scalability",
            "2026-06-13T02:00:00Z", "abc1234",
            Map.of("numberOfTopics", 250),
            Map.of("reconciliationIntervalMs", 74596.0)
        );
        mapper.writeValue(resultsDir.resolve("topic-operator-scalability.json").toFile(), result);

        Files.createDirectories(repoDir.resolve("regressions"));

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, 10, 2.0);
        List<RegressionResult> results = comparator.compareLatest();

        assertTrue(results.isEmpty());

        Path baselinesFile = repoDir.resolve("baselines.json");
        assertTrue(Files.exists(baselinesFile));

        Map<String, Map<String, BaselineMetric>> baselines = mapper.readValue(
            baselinesFile.toFile(),
            new TypeReference<>() { }
        );
        assertTrue(baselines.containsKey("topic-operator-scalability"));
        assertEquals(74596.0, baselines.get("topic-operator-scalability").get("reconciliationIntervalMs").getMean(), 0.01);
    }

    @Test
    void testDetectsRegression(@TempDir Path repoDir) throws Exception {
        Map<String, Map<String, BaselineMetric>> baselines = new LinkedHashMap<>();
        BaselineMetric metric = new BaselineMetric(10);
        for (int i = 0; i < 5; i++) {
            metric.addValue(70000.0 + (i * 1000));
        }
        baselines.put("topic-operator-scalability", Map.of("reconciliationIntervalMs", metric));

        Files.createDirectories(repoDir.resolve("regressions"));
        mapper.writeValue(repoDir.resolve("baselines.json").toFile(), baselines);

        Path resultsDir = repoDir.resolve("results").resolve("2026-06-14");
        Files.createDirectories(resultsDir);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator", "scalability",
            "2026-06-14T02:00:00Z", "def5678",
            Map.of("numberOfTopics", 250),
            Map.of("reconciliationIntervalMs", 150000.0)
        );
        mapper.writeValue(resultsDir.resolve("topic-operator-scalability.json").toFile(), result);

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, 10, 2.0);
        List<RegressionResult> results = comparator.compareLatest();

        assertEquals(1, results.size());
        assertFalse(results.get(0).isPassed());
        assertEquals("reconciliationIntervalMs", results.get(0).getMetricName());
    }

    @Test
    void testPassesWithinThreshold(@TempDir Path repoDir) throws Exception {
        Map<String, Map<String, BaselineMetric>> baselines = new LinkedHashMap<>();
        BaselineMetric metric = new BaselineMetric(10);
        for (int i = 0; i < 5; i++) {
            metric.addValue(70000.0 + (i * 1000));
        }
        baselines.put("topic-operator-scalability", Map.of("reconciliationIntervalMs", metric));

        Files.createDirectories(repoDir.resolve("regressions"));
        mapper.writeValue(repoDir.resolve("baselines.json").toFile(), baselines);

        Path resultsDir = repoDir.resolve("results").resolve("2026-06-14");
        Files.createDirectories(resultsDir);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator", "scalability",
            "2026-06-14T02:00:00Z", "def5678",
            Map.of("numberOfTopics", 250),
            Map.of("reconciliationIntervalMs", 73000.0)
        );
        mapper.writeValue(resultsDir.resolve("topic-operator-scalability.json").toFile(), result);

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, 10, 2.0);
        List<RegressionResult> results = comparator.compareLatest();

        assertEquals(1, results.size());
        assertTrue(results.get(0).isPassed());
    }

    @Test
    void testUpdatesBaselineAfterComparison(@TempDir Path repoDir) throws Exception {
        Map<String, Map<String, BaselineMetric>> baselines = new LinkedHashMap<>();
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(70000.0);
        baselines.put("topic-operator-scalability", Map.of("reconciliationIntervalMs", metric));

        Files.createDirectories(repoDir.resolve("regressions"));
        mapper.writeValue(repoDir.resolve("baselines.json").toFile(), baselines);

        Path resultsDir = repoDir.resolve("results").resolve("2026-06-14");
        Files.createDirectories(resultsDir);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator", "scalability",
            "2026-06-14T02:00:00Z", "def5678",
            Map.of("numberOfTopics", 250),
            Map.of("reconciliationIntervalMs", 72000.0)
        );
        mapper.writeValue(resultsDir.resolve("topic-operator-scalability.json").toFile(), result);

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, 10, 2.0);
        comparator.compareLatest();

        Map<String, Map<String, BaselineMetric>> updated = mapper.readValue(
            repoDir.resolve("baselines.json").toFile(),
            new TypeReference<>() { }
        );

        List<Double> window = updated.get("topic-operator-scalability").get("reconciliationIntervalMs").getWindow();
        assertEquals(2, window.size());
        assertEquals(72000.0, window.get(1));
    }

    @Test
    void testWritesCurrentRegressions(@TempDir Path repoDir) throws Exception {
        Map<String, Map<String, BaselineMetric>> baselines = new LinkedHashMap<>();
        BaselineMetric metric = new BaselineMetric(10);
        for (int i = 0; i < 5; i++) {
            metric.addValue(70000.0);
        }
        baselines.put("topic-operator-scalability", Map.of("reconciliationIntervalMs", metric));

        Files.createDirectories(repoDir.resolve("regressions"));
        mapper.writeValue(repoDir.resolve("baselines.json").toFile(), baselines);

        Path resultsDir = repoDir.resolve("results").resolve("2026-06-14");
        Files.createDirectories(resultsDir);

        TestResult result = new TestResult(
            "TopicOperatorScalabilityPerformance",
            "topic-operator", "scalability",
            "2026-06-14T02:00:00Z", "def5678",
            Map.of("numberOfTopics", 250),
            Map.of("reconciliationIntervalMs", 200000.0)
        );
        mapper.writeValue(resultsDir.resolve("topic-operator-scalability.json").toFile(), result);

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, 10, 2.0);
        comparator.compareLatest();

        Path regressionsFile = repoDir.resolve("regressions").resolve("current.json");
        assertTrue(Files.exists(regressionsFile));

        String content = Files.readString(regressionsFile);
        assertTrue(content.contains("reconciliationIntervalMs"));
        assertTrue(content.contains("200000"));
    }
}
