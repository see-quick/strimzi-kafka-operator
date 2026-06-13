/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class BaselineMetric {

    private double mean;
    private double stddev;
    private List<Double> window;
    private int windowSize;

    @JsonCreator
    public BaselineMetric(@JsonProperty("windowSize") int windowSize) {
        this.windowSize = windowSize;
        this.window = new ArrayList<>();
        this.mean = 0.0;
        this.stddev = 0.0;
    }

    public void addValue(double value) {
        window.add(value);
        if (window.size() > windowSize) {
            window.remove(0);
        }
        recompute();
    }

    private void recompute() {
        if (window.isEmpty()) {
            mean = 0.0;
            stddev = 0.0;
            return;
        }

        mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

        double variance = window.stream()
            .mapToDouble(v -> (v - mean) * (v - mean))
            .average()
            .orElse(0.0);
        stddev = Math.sqrt(variance);

        double minStddev = Math.abs(mean) * 0.05;
        if (stddev < minStddev) {
            stddev = minStddev;
        }
    }

    public boolean isRegression(double currentValue, double threshold) {
        return getDeviations(currentValue) > threshold;
    }

    public double getDeviations(double currentValue) {
        if (stddev == 0.0) {
            return 0.0;
        }
        return (currentValue - mean) / stddev;
    }

    public double getMean() {
        return mean;
    }

    public double getStddev() {
        return stddev;
    }

    public List<Double> getWindow() {
        return new ArrayList<>(window);
    }

    public int getWindowSize() {
        return windowSize;
    }
}
