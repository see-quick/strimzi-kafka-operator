/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class TestResult {

    private final String testName;
    private final String component;
    private final String useCase;
    private final String timestamp;
    private final String commitSha;
    private final Map<String, Object> parameters;
    private final Map<String, Double> metrics;

    @JsonCreator
    public TestResult(
        @JsonProperty("testName") String testName,
        @JsonProperty("component") String component,
        @JsonProperty("useCase") String useCase,
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("commitSha") String commitSha,
        @JsonProperty("parameters") Map<String, Object> parameters,
        @JsonProperty("metrics") Map<String, Double> metrics
    ) {
        this.testName = testName;
        this.component = component;
        this.useCase = useCase;
        this.timestamp = timestamp;
        this.commitSha = commitSha;
        this.parameters = parameters;
        this.metrics = metrics;
    }

    public String getTestName() {
        return testName;
    }

    public String getComponent() {
        return component;
    }

    public String getUseCase() {
        return useCase;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getCommitSha() {
        return commitSha;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Map<String, Double> getMetrics() {
        return metrics;
    }
}
