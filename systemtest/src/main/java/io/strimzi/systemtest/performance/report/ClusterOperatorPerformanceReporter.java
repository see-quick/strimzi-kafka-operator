/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Map;

public class ClusterOperatorPerformanceReporter extends BasePerformanceReporter {

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorPerformanceReporter.class);

    @Override
    protected Path resolveComponentUseCasePathDir(Path performanceLogDir, String useCaseName, Map<String, Object> performanceAttributes) {
        final String brokerCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, "").toString();
        final String controllerCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, "").toString();

        StringBuilder dirPathBuilder = new StringBuilder();
        dirPathBuilder.append(useCaseName);

        if (!brokerCount.isEmpty()) {
            dirPathBuilder.append("/brokers-").append(brokerCount);
        }
        if (!controllerCount.isEmpty()) {
            dirPathBuilder.append("-controllers-").append(controllerCount);
        }

        // Append use-case specific suffixes
        String connectorCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CONNECTOR_COUNT, "").toString();
        if (!connectorCount.isEmpty()) {
            dirPathBuilder.append("-connectors-").append(connectorCount);
        }

        String caType = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_CA_TYPE, "").toString();
        if (!caType.isEmpty()) {
            dirPathBuilder.append("-ca-").append(caType);
        }

        String topicCount = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_TOPIC_COUNT, "").toString();
        if (!topicCount.isEmpty()) {
            dirPathBuilder.append("-topics-").append(topicCount);
        }

        String initialBrokers = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_INITIAL_BROKER_COUNT, "").toString();
        String scaledBrokers = performanceAttributes.getOrDefault(PerformanceConstants.CLUSTER_OPERATOR_IN_SCALED_BROKER_COUNT, "").toString();
        if (!initialBrokers.isEmpty() && !scaledBrokers.isEmpty()) {
            dirPathBuilder.append("-scale-").append(initialBrokers).append("-to-").append(scaledBrokers);
        }

        final Path clusterOperatorUseCasePathDir = performanceLogDir.resolve(dirPathBuilder.toString());

        LOGGER.info("Resolved CO performance log directory: {} for use case '{}'", clusterOperatorUseCasePathDir, useCaseName);

        return clusterOperatorUseCasePathDir;
    }
}
