/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import io.strimzi.api.kafka.model.kafka.KafkaSpec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents the metrics collected from an experiment run within the Strimzi test framework.
 * It includes a collection of simple key-value metrics and a detailed specification of the Kafka cluster used during the experiment.
 */
public class ExperimentMetrics {
    /**
     * A map to hold simple metrics with their keys and values as strings.
     */
    private Map<String, String> simpleMetrics;

    /**
     * The specification of the Kafka cluster used during the experiment.
     */
    private KafkaSpec kafkaSpec;

    /**
     * Initializes a new instance of {@code ExperimentMetrics} with an empty set of simple metrics
     * and a new instance of {@code KafkaSpec}.
     */
    public ExperimentMetrics() {
        this.simpleMetrics = new LinkedHashMap<>();
        this.kafkaSpec = new KafkaSpec();
    }

    /**
     * Adds a simple metric to the collection with a specified key and value.
     *
     * @param key the key of the metric to add
     * @param value the value of the metric
     */
    public void addSimpleMetric(String key, String value) {
        this.simpleMetrics.put(key, value);
    }

    /**
     * Sets the Kafka specification for the experiment metrics.
     *
     * @param kafkaSpec the Kafka specification to set
     */
    public void setKafkaSpec(KafkaSpec kafkaSpec) {
        this.kafkaSpec = kafkaSpec;
    }

    public Map<String, String> getSimpleMetrics() {
        return this.simpleMetrics;
    }

    public KafkaSpec getKafkaSpec() {
        return this.kafkaSpec;
    }

    @Override
    public String toString() {
        return "PerformanceMetrics{" +
                "simpleMetrics=" + this.simpleMetrics +
                '}';
    }
}
