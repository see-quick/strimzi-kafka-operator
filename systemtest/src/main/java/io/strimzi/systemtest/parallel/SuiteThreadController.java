/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.Semaphore;

/**
 * Class responsible for synchronization of parallel tests using Semaphore.
 */
public class SuiteThreadController {

    private static final Logger LOGGER = LogManager.getLogger(SuiteThreadController.class);
    private static final String JUNIT_PARALLEL_COUNT_PROPERTY_NAME = "junit.jupiter.execution.parallel.config.fixed.parallelism";

    private static Semaphore numberOfTestsToRun;
    private static SuiteThreadController instance;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
            int numberOfTestsInParallel;
            if (System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME) != null) {
                numberOfTestsInParallel =  Integer.parseInt((String) System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME));
                LOGGER.info("Going to execute {} tests in parallel", numberOfTestsInParallel);
            } else {
                LOGGER.warn("User did not specify junit.jupiter.execution.parallel.config.fixed.parallelism " +
                    "in junit-platform.properties gonna use default as 1 (sequence mode)");
                numberOfTestsInParallel = 1;
            }
            numberOfTestsToRun = new Semaphore(numberOfTestsInParallel);
        }
        return instance;
    }

    public void addParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Adding parallel test: {}",
                StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()),
                extensionContext.getDisplayName());

        try {
            numberOfTestsToRun.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        LOGGER.debug("[{}] - Available permits: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()),
                numberOfTestsToRun.availablePermits());
    }

    public void removeParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Removing parallel test: {}",
                StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()),
                extensionContext.getDisplayName());

        numberOfTestsToRun.release();

        LOGGER.debug("[{}] - Available permits: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()),
                numberOfTestsToRun.availablePermits());
    }
}
