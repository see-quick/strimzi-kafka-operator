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

        final Path clusterOperatorUseCasePathDir = performanceLogDir.resolve(dirPathBuilder.toString());

        LOGGER.info("Resolved CO performance log directory: {} for use case '{}'", clusterOperatorUseCasePathDir, useCaseName);

        return clusterOperatorUseCasePathDir;
    }
}
