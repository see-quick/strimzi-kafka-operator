/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultExporter {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
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
}
