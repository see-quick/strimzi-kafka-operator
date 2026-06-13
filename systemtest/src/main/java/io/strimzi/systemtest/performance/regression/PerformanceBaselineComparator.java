/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PerformanceBaselineComparator {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Path repoDir;
    private final int windowSize;
    private final double threshold;

    public PerformanceBaselineComparator(Path repoDir, int windowSize, double threshold) {
        this.repoDir = repoDir;
        this.windowSize = windowSize;
        this.threshold = threshold;
    }

    public List<RegressionResult> compareLatest() throws IOException {
        Map<String, Map<String, BaselineMetric>> baselines = loadBaselines();
        Map<String, TestResult> latestResults = loadLatestResults();
        boolean isFirstRun = baselines.isEmpty();

        List<RegressionResult> allResults = new ArrayList<>();

        for (Map.Entry<String, TestResult> entry : latestResults.entrySet()) {
            String testKey = entry.getKey();
            TestResult result = entry.getValue();

            Map<String, BaselineMetric> testBaseline = baselines.computeIfAbsent(testKey, k -> new LinkedHashMap<>());

            for (Map.Entry<String, Double> metricEntry : result.getMetrics().entrySet()) {
                String metricName = metricEntry.getKey();
                double currentValue = metricEntry.getValue();

                BaselineMetric baseline = testBaseline.get(metricName);

                if (baseline != null && !isFirstRun) {
                    double deviations = baseline.getDeviations(currentValue);
                    boolean passed = !baseline.isRegression(currentValue, threshold);

                    allResults.add(new RegressionResult(
                        result.getTestName(), metricName, currentValue,
                        baseline.getMean(), baseline.getStddev(),
                        deviations, threshold, passed
                    ));
                }

                if (baseline == null) {
                    baseline = new BaselineMetric(windowSize);
                    testBaseline.put(metricName, baseline);
                }
                baseline.addValue(currentValue);
            }
        }

        saveBaselines(baselines);
        saveRegressions(allResults);
        printReport(allResults, latestResults);

        return allResults;
    }

    private Map<String, Map<String, BaselineMetric>> loadBaselines() throws IOException {
        Path baselinesFile = repoDir.resolve("baselines.json");
        if (!Files.exists(baselinesFile)) {
            return new LinkedHashMap<>();
        }
        return MAPPER.readValue(baselinesFile.toFile(), new TypeReference<>() { });
    }

    private Map<String, TestResult> loadLatestResults() throws IOException {
        Path resultsDir = repoDir.resolve("results");
        if (!Files.exists(resultsDir)) {
            return Map.of();
        }

        File[] dateDirs = resultsDir.toFile().listFiles(File::isDirectory);
        if (dateDirs == null || dateDirs.length == 0) {
            return Map.of();
        }

        Arrays.sort(dateDirs, Comparator.comparing(File::getName));
        File latestDir = dateDirs[dateDirs.length - 1];

        Map<String, TestResult> results = new LinkedHashMap<>();
        File[] jsonFiles = latestDir.listFiles((dir, name) -> name.endsWith(".json") && !name.equals("metadata.json"));
        if (jsonFiles == null) {
            return results;
        }

        for (File jsonFile : jsonFiles) {
            TestResult result = MAPPER.readValue(jsonFile, TestResult.class);
            String key = jsonFile.getName().replace(".json", "");
            results.put(key, result);
        }

        return results;
    }

    private void saveBaselines(Map<String, Map<String, BaselineMetric>> baselines) throws IOException {
        MAPPER.writeValue(repoDir.resolve("baselines.json").toFile(), baselines);
    }

    private void saveRegressions(List<RegressionResult> results) throws IOException {
        List<RegressionResult> failures = results.stream().filter(r -> !r.isPassed()).toList();

        Map<String, Object> regressionReport = new LinkedHashMap<>();
        regressionReport.put("detectedAt", Instant.now().toString());
        regressionReport.put("regressions", failures);

        Path regressionsDir = repoDir.resolve("regressions");
        Files.createDirectories(regressionsDir);
        MAPPER.writeValue(regressionsDir.resolve("current.json").toFile(), regressionReport);
    }

    private void printReport(List<RegressionResult> results, Map<String, TestResult> latestResults) {
        if (results.isEmpty()) {
            System.out.println("=== Performance Baseline Initialized ===");
            System.out.println("First run recorded. No comparison available yet.");
            System.out.println("Tests recorded: " + latestResults.size());
            return;
        }

        String commitSha = latestResults.values().stream()
            .findFirst()
            .map(TestResult::getCommitSha)
            .orElse("unknown");

        System.out.println("=== Performance Regression Report ===");
        System.out.println("Commit: " + commitSha + " | Date: " + Instant.now().toString().substring(0, 10));
        System.out.println();

        for (RegressionResult result : results) {
            System.out.println(result.toString());
        }

        long failures = results.stream().filter(r -> !r.isPassed()).count();
        System.out.println();
        if (failures > 0) {
            System.out.printf("Result: REGRESSION DETECTED (%d of %d metrics)%n", failures, results.size());
        } else {
            System.out.printf("Result: ALL PASSED (%d metrics)%n", results.size());
        }
    }

    public static void main(String[] args) throws IOException {
        Path repoDir = null;
        int windowSize = 10;
        double thresh = 2.0;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--results-repo":
                    repoDir = Path.of(args[++i]);
                    break;
                case "--window-size":
                    windowSize = Integer.parseInt(args[++i]);
                    break;
                case "--threshold":
                    thresh = Double.parseDouble(args[++i]);
                    break;
                default:
                    break;
            }
        }

        if (repoDir == null) {
            System.err.println("Usage: PerformanceBaselineComparator --results-repo <path> [--window-size N] [--threshold K]");
            System.exit(1);
        }

        PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(repoDir, windowSize, thresh);
        List<RegressionResult> results = comparator.compareLatest();

        boolean hasRegression = results.stream().anyMatch(r -> !r.isPassed());
        System.exit(hasRegression ? 1 : 0);
    }
}
