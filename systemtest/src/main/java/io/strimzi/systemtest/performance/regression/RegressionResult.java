/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RegressionResult {

    private final String testName;
    private final String metricName;
    private final double currentValue;
    private final double baselineMean;
    private final double baselineStddev;
    private final double deviations;
    private final double threshold;
    private final boolean passed;

    @JsonCreator
    public RegressionResult(
        @JsonProperty("testName") String testName,
        @JsonProperty("metricName") String metricName,
        @JsonProperty("currentValue") double currentValue,
        @JsonProperty("baselineMean") double baselineMean,
        @JsonProperty("baselineStddev") double baselineStddev,
        @JsonProperty("deviations") double deviations,
        @JsonProperty("threshold") double threshold,
        @JsonProperty("passed") boolean passed
    ) {
        this.testName = testName;
        this.metricName = metricName;
        this.currentValue = currentValue;
        this.baselineMean = baselineMean;
        this.baselineStddev = baselineStddev;
        this.deviations = deviations;
        this.threshold = threshold;
        this.passed = passed;
    }

    public String getTestName() {
        return testName;
    }

    public String getMetricName() {
        return metricName;
    }

    public double getCurrentValue() {
        return currentValue;
    }

    public double getBaselineMean() {
        return baselineMean;
    }

    public double getBaselineStddev() {
        return baselineStddev;
    }

    public double getDeviations() {
        return deviations;
    }

    public double getThreshold() {
        return threshold;
    }

    public boolean isPassed() {
        return passed;
    }

    @Override
    public String toString() {
        String status = passed ? "[PASS]" : "[FAIL]";
        return String.format("%s %s / %s: %.1f (baseline: %.1f +/- %.1f, %.2f sigma)",
            status, testName, metricName, currentValue, baselineMean, baselineStddev, deviations);
    }
}
