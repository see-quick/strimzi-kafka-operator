/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.parallel.ParallelNamespacesSuitesNames;
import io.strimzi.systemtest.parallel.ParallelSuiteController;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

/**
 * Custom Extension which executes code only once before all tests are started and after all tests finished.
 * This is temporary solution until https://github.com/junit-team/junit5/issues/456 will not be released
 */
public class BeforeAllOnce implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static SetupClusterOperator install;
    private static final Logger LOGGER = LogManager.getLogger(BeforeAllOnce.class);
    private static boolean systemReady = false;
    private static ExtensionContext sharedExtensionContext;

    /**
     * Separate method with 'synchronized static' required for make sure procedure will be executed
     * only once across all simultaneously running threads
     */
    synchronized private static void systemSetup(ExtensionContext extensionContext) throws Exception {
        // 'if' is used to make sure procedure will be executed only once, not before every class
        if (!systemReady) {
            // get root extension context to be different from others context (BeforeAll)
            sharedExtensionContext = extensionContext.getRoot();
            systemReady = true;
            LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
            LOGGER.debug("[BEFORE SUITE] - Going to setup testing system");

            // When we set RBAC policy to NAMESPACE, we must copy all Roles to other (parallel) namespaces.
            if (Environment.isNamespaceRbacScope() && !Environment.isHelmInstall()) {
                LOGGER.debug("Watched namespaces: :\n{}", ParallelNamespacesSuitesNames.getRbacNamespacesToWatch());

                // setup cluster operator before all suites only once
                install = new SetupClusterOperator.SetupClusterOperatorBuilder()
                    .withExtensionContext(sharedExtensionContext)
                    .withNamespace(Constants.INFRA_NAMESPACE)
                    .withWatchingNamespaces(ParallelNamespacesSuitesNames.getRbacNamespacesToWatch())
                    .withBindingsNamespaces(ParallelNamespacesSuitesNames.getBindingNamespaces())
                    .createInstallation()
                    .runInstallation();
            } else {
                // setup cluster operator before all suites only once
                install = new SetupClusterOperator.SetupClusterOperatorBuilder()
                    .withExtensionContext(sharedExtensionContext)
                    .withNamespace(Constants.INFRA_NAMESPACE)
                    .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                    .withBindingsNamespaces(ParallelNamespacesSuitesNames.getBindingNamespaces())
                    .createInstallation()
                    .runInstallation();
            }
            // this is for correction because callback also count some randomly chosen class twice
            ParallelSuiteController.decrementCounter();
        }
    }

    /**
     * Initial setup of system. Including configuring services,
     * adding calls to callrec, users to scorecard, call media files
     *
     * @param extensionContext junit context
     */
    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        systemSetup(extensionContext);
    }

    /**
     * CloseableResource implementation, adding value into GLOBAL context is required to registers a callback hook
     * With such steps close() method will be executed only once in the end of test execution
     */
    @Override
    public void close() throws Exception {
        // clean data from system
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [AFTER SUITE] has been called", this.getClass().getName());
        install.unInstall();
    }

    public static ExtensionContext getSharedExtensionContext() {
        return sharedExtensionContext;
    }

    public static SetupClusterOperator getInstall() {
        return install;
    }
}