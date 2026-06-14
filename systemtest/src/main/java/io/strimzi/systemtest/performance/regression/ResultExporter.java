/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.strimzi.systemtest.performance.report.parser.BasePerformanceMetricsParser;
import io.strimzi.systemtest.performance.report.parser.ExperimentMetrics;
import io.strimzi.systemtest.performance.report.parser.ParserFactory;
import io.strimzi.systemtest.performance.report.parser.ParserType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultExporter {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final DateTimeFormatter DIR_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH").withZone(ZoneOffset.UTC);
    private static final Pattern UNIT_SUFFIX = Pattern.compile("\\s*\\(([^)]+)\\)\\s*$");

    public static TestResult convertMetrics(
        Map<String, String> testMetrics,
        String component,
        String useCase,
        String timestamp,
        String commitSha
    ) {
        Map<String, Object> parameters = new LinkedHashMap<>();
        Map<String, Double> metrics = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : testMetrics.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith("OUT:")) {
                String normalizedKey = normalizeMetricKey(key);
                try {
                    metrics.put(normalizedKey, Double.parseDouble(value.trim()));
                } catch (NumberFormatException e) {
                    // skip non-numeric OUT values
                }
            } else if (key.startsWith("IN:") && !key.contains("Kafka Configuration")) {
                String normalizedKey = normalizeMetricKey(key);
                try {
                    parameters.put(normalizedKey, Integer.parseInt(value.trim()));
                } catch (NumberFormatException e) {
                    parameters.put(normalizedKey, value.trim());
                }
            }
        }

        String testName = buildTestName(component, useCase);
        return new TestResult(testName, component, useCase, timestamp, commitSha, parameters, metrics);
    }

    public static void writeResult(TestResult result, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        String fileName = result.getComponent() + "-" + result.getUseCase() + ".json";
        MAPPER.writeValue(outputDir.resolve(fileName).toFile(), result);
    }

    public static void writeMetadata(ResultMetadata metadata, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        MAPPER.writeValue(outputDir.resolve("metadata.json").toFile(), metadata);
    }

    static String normalizeMetricKey(String rawKey) {
        String key = rawKey;
        if (key.startsWith("OUT:") || key.startsWith("IN:")) {
            key = key.substring(key.indexOf(':') + 1).trim();
        }

        String unitSuffix = "";
        Matcher unitMatcher = UNIT_SUFFIX.matcher(key);
        if (unitMatcher.find()) {
            unitSuffix = unitMatcher.group(1).trim();
            key = unitMatcher.replaceAll("");
        }

        String[] words = key.trim().split("[\\s_]+");
        StringBuilder camelCase = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String word = words[i].toLowerCase();
            if (i == 0) {
                camelCase.append(word);
            } else {
                camelCase.append(Character.toUpperCase(word.charAt(0)));
                camelCase.append(word.substring(1));
            }
        }

        if (!unitSuffix.isEmpty()) {
            String normalizedUnit = unitSuffix.replaceAll("[^a-zA-Z]", "");
            camelCase.append(Character.toUpperCase(normalizedUnit.charAt(0)));
            camelCase.append(normalizedUnit.substring(1).toLowerCase());
        }

        return camelCase.toString();
    }

    private static String buildTestName(String component, String useCase) {
        String[] componentParts = component.split("-");
        StringBuilder name = new StringBuilder();
        for (String part : componentParts) {
            name.append(Character.toUpperCase(part.charAt(0)));
            name.append(part.substring(1));
        }

        String[] useCaseParts = useCase.replace("UseCase", "").split("(?=[A-Z])");
        for (String part : useCaseParts) {
            if (!part.isEmpty()) {
                name.append(Character.toUpperCase(part.charAt(0)));
                name.append(part.substring(1));
            }
        }
        name.append("Performance");
        return name.toString();
    }

    public static void exportFromParserOutput(Path outputDir, String commitSha) throws IOException {
        String timestamp = Instant.now().toString();
        int exported = 0;

        for (ParserType parserType : ParserType.values()) {
            String component = parserType.getParserName();
            try {
                BasePerformanceMetricsParser parser = ParserFactory.createParser(parserType);
                parser.parseMetrics();

                Map<String, List<ExperimentMetrics>> experiments = parser.getUseCaseExperiments();
                if (experiments == null || experiments.isEmpty()) {
                    continue;
                }

                for (Map.Entry<String, List<ExperimentMetrics>> entry : experiments.entrySet()) {
                    String useCase = entry.getKey();
                    List<ExperimentMetrics> experimentList = entry.getValue();

                    for (ExperimentMetrics experiment : experimentList) {
                        TestResult result = convertMetrics(
                            experiment.getTestMetrics(),
                            component,
                            useCase,
                            timestamp,
                            commitSha
                        );

                        if (!result.getMetrics().isEmpty()) {
                            writeResult(result, outputDir);
                            exported++;
                            System.out.printf("Exported: %s / %s (%d metrics)%n",
                                component, useCase, result.getMetrics().size());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.printf("No data for %s (skipping): %s%n", component, e.getMessage());
            }
        }

        System.out.printf("%nTotal exported: %d test results%n", exported);
    }

    public static void main(String[] args) throws IOException {
        Path outputDir = null;
        String commitSha = "unknown";
        String resultsRepo = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--output-dir":
                    outputDir = Path.of(args[++i]);
                    break;
                case "--commit":
                    commitSha = args[++i];
                    break;
                case "--results-repo":
                    resultsRepo = args[++i];
                    break;
                default:
                    break;
            }
        }

        if (resultsRepo != null && outputDir == null) {
            outputDir = Path.of(resultsRepo, "results", DIR_DATE_FORMAT.format(Instant.now()));
        }

        if (outputDir == null) {
            System.err.println("Usage: ResultExporter --results-repo <path> [--output-dir <path>] [--commit <sha>]");
            System.exit(1);
        }

        System.out.println("=== Exporting Performance Results ===");
        System.out.println("Output: " + outputDir);
        System.out.println("Commit: " + commitSha);
        System.out.println();

        exportFromParserOutput(outputDir, commitSha);

        if (resultsRepo != null) {
            System.out.println();
            System.out.println("=== Running Baseline Comparison ===");
            PerformanceBaselineComparator comparator = new PerformanceBaselineComparator(
                Path.of(resultsRepo), 10, 2.0
            );
            List<RegressionResult> results = comparator.compareLatest();
            boolean hasRegression = results.stream().anyMatch(r -> !r.isPassed());
            if (hasRegression) {
                System.exit(1);
            }
        }
    }
}
