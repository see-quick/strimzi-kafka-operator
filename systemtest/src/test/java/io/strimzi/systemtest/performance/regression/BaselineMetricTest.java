/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaselineMetricTest {

    @Test
    void testAddValueAndRecompute() {
        BaselineMetric metric = new BaselineMetric(5);
        metric.addValue(10.0);
        metric.addValue(20.0);
        metric.addValue(30.0);

        assertEquals(20.0, metric.getMean(), 0.01);
        assertEquals(3, metric.getWindow().size());
    }

    @Test
    void testWindowEvictsOldestWhenFull() {
        BaselineMetric metric = new BaselineMetric(3);
        metric.addValue(10.0);
        metric.addValue(20.0);
        metric.addValue(30.0);
        metric.addValue(40.0);

        assertEquals(3, metric.getWindow().size());
        assertEquals(List.of(20.0, 30.0, 40.0), metric.getWindow());
        assertEquals(30.0, metric.getMean(), 0.01);
    }

    @Test
    void testStddevComputation() {
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(10.0);
        metric.addValue(10.0);
        metric.addValue(10.0);

        assertEquals(0.5, metric.getStddev(), 0.01);
    }

    @Test
    void testStddevWithVariance() {
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(10.0);
        metric.addValue(20.0);
        metric.addValue(30.0);

        double expectedMean = 20.0;
        assertEquals(expectedMean, metric.getMean(), 0.01);
        assertEquals(8.165, metric.getStddev(), 0.01);
    }

    @Test
    void testIsRegressionAboveThreshold() {
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(100.0);
        metric.addValue(102.0);
        metric.addValue(98.0);
        metric.addValue(101.0);
        metric.addValue(99.0);

        assertTrue(metric.isRegression(200.0, 2.0));
    }

    @Test
    void testIsNotRegressionWithinThreshold() {
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(100.0);
        metric.addValue(102.0);
        metric.addValue(98.0);
        metric.addValue(101.0);
        metric.addValue(99.0);

        assertFalse(metric.isRegression(101.0, 2.0));
    }

    @Test
    void testDeviations() {
        BaselineMetric metric = new BaselineMetric(10);
        metric.addValue(100.0);
        metric.addValue(100.0);
        metric.addValue(100.0);

        assertEquals(4.0, metric.getDeviations(120.0), 0.01);
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        BaselineMetric metric = new BaselineMetric(5);
        metric.addValue(10.0);
        metric.addValue(20.0);
        metric.addValue(30.0);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(metric);
        BaselineMetric deserialized = mapper.readValue(json, BaselineMetric.class);

        assertEquals(metric.getMean(), deserialized.getMean(), 0.01);
        assertEquals(metric.getStddev(), deserialized.getStddev(), 0.01);
        assertEquals(metric.getWindow(), deserialized.getWindow());
        assertEquals(metric.getWindowSize(), deserialized.getWindowSize());
    }
}
