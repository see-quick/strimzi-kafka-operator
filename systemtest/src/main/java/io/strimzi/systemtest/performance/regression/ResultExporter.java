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

        String testName = buildTestName(component, useCase, parameters);
        return new TestResult(testName, component, useCase, timestamp, commitSha, parameters, metrics);
    }

    public static void writeResult(TestResult result, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        String suffix = buildParameterSuffix(result.getParameters());
        String fileName = result.getComponent() + "-" + result.getUseCase() + suffix + ".json";
        MAPPER.writeValue(outputDir.resolve(fileName).toFile(), result);
    }

    static String buildParameterSuffix(Map<String, Object> parameters) {
        String key = findDistinguishingParameter(parameters);
        if (key == null) {
            return "";
        }
        Object value = parameters.get(key);
        return "-" + key + "-" + value;
    }

    private static String findDistinguishingParameter(Map<String, Object> parameters) {
        for (String candidate : List.of("numberOfTopics", "numberOfKafkaUsers", "connectorCount")) {
            if (parameters.containsKey(candidate)) {
                return candidate;
            }
        }
        return null;
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

    private static String buildTestName(String component, String useCase, Map<String, Object> parameters) {
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

        String distinguishing = findDistinguishingParameter(parameters);
        if (distinguishing != null) {
            name.append(" (").append(distinguishing).append("=").append(parameters.get(distinguishing)).append(")");
        }

        return name.toString();
    }

    /**
     * Derives summary statistics from the raw operator time-series that the metrics
     * schedulers scrape during each experiment (JVM memory, CPU, GC pauses,
     * reconciliation durations). The parser already loads these files into
     * {@link ExperimentMetrics#getComponentMetrics()}; without this step they would
     * be collected and then thrown away.
     *
     * @param componentMetrics  raw metric file name to sampled values, as parsed
     * @return                  derived metric name to summary value
     */
    static Map<String, Double> deriveComponentMetrics(Map<String, List<Double>> componentMetrics) {
        Map<String, Double> derived = new LinkedHashMap<>();

        List<Double> memory = componentMetrics.get("jvm_memory_used_megabytes_total.txt");
        putIfPresent(derived, "jvmMemoryUsedMaxMb", maxOf(memory));
        putIfPresent(derived, "jvmMemoryUsedAvgMb", avgOf(memory));

        List<Double> cpu = componentMetrics.get("process_cpu_usage.txt");
        putIfPresent(derived, "processCpuUsageMax", maxOf(cpu));
        putIfPresent(derived, "processCpuUsageAvg", avgOf(cpu));

        putIfPresent(derived, "jvmThreadsLiveMax", maxOf(componentMetrics.get("jvm_threads_live_threads.txt")));

        // GC pause samples are persisted per action/cause/gc tag combination,
        // e.g. "action=end of major GC,cause=Allocation Failure,gc=MarkSweepCompact.txt"
        Double gcPauseMax = maxOf(componentMetrics.entrySet().stream()
            .filter(e -> e.getKey().startsWith("action="))
            .flatMap(e -> e.getValue().stream())
            .toList());
        putIfPresent(derived, "jvmGcPauseMaxSeconds", gcPauseMax);

        putIfPresent(derived, "reconciliationDurationMaxSeconds",
            maxOf(componentMetrics.get("strimzi_reconciliations_duration_seconds_max.txt")));

        // sum and total are cumulative counters, so their max is the latest scrape
        Double durationSum = maxOf(componentMetrics.get("strimzi_reconciliations_duration_seconds_sum.txt"));
        Double reconciliations = maxOf(componentMetrics.get("strimzi_reconciliations_total.txt"));
        if (durationSum != null && reconciliations != null && reconciliations > 0) {
            putIfPresent(derived, "reconciliationDurationAvgSeconds", durationSum / reconciliations);
        }

        putIfPresent(derived, "reconciliationsFailedTotal",
            maxOf(componentMetrics.get("strimzi_reconciliations_failed_total.txt")));

        return derived;
    }

    private static final List<String> INFORMATIONAL_METRIC_PREFIXES =
        List.of("jvm", "processCpu", "reconciliationDuration", "reconciliationsFailed");

    /**
     * Resource and JVM health metrics derived by {@link #deriveComponentMetrics(Map)} are
     * exported for the dashboard but are too noisy night-to-night for the sigma-based
     * regression gate; the baseline comparator tracks their baselines without flagging them.
     *
     * @param metricName    metric name as exported
     * @return              true when the metric is informational (not regression-gated)
     */
    public static boolean isInformationalMetric(String metricName) {
        return INFORMATIONAL_METRIC_PREFIXES.stream().anyMatch(metricName::startsWith);
    }

    private static Double maxOf(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
    }

    private static Double avgOf(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    private static void putIfPresent(Map<String, Double> metrics, String key, Double value) {
        if (value != null) {
            metrics.put(key, Math.round(value * 10000.0) / 10000.0);
        }
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
                            result.getMetrics().putAll(deriveComponentMetrics(experiment.getComponentMetrics()));
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
            comparator.compareLatest();
        }
    }
}
