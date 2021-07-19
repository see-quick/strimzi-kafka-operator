/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.k8s.KubeClusterResource;
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

    private static final Logger LOGGER = LogManager.getLogger(BeforeAllOnce.class);
    private static boolean systemReady = false;
    private static SetupClusterOperator install;

    /**
     * Separate method with 'synchronized static' required for make sure procedure will be executed
     * only once across all simultaneously running threads
     */
    synchronized private static void systemSetup(ExtensionContext extensionContext) throws Exception {
        // 'if' is used to make sure procedure will be executed only once, not before every class
        if (!systemReady) {
            systemReady = true;
            LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
            LOGGER.debug("{} - [BEFORE SUITE] has been called", BeforeAllOnce.class.getName());

            // setup cluster operator before all suites only once
            install = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(extensionContext)
                .withNamespace(Constants.INFRA_NAMESPACE)
                .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                .createInstallation()
                .runInstallation();
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
        extensionContext.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put(systemReady, this);
    }

    /**
     * CloseableResource implementation, adding value into GLOBAL context is required to  registers a callback hook
     * With such steps close() method will be executed only once in the end of test execution
     */
    @Override
    public void close() throws Exception {
        // clean data from system
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("{} - [AFTER SUITE] has been called", this.getClass().getName());
        // Clear cluster from all created namespaces and configurations files for cluster operator.
        install.deleteClusterOperatorInstallFiles();
        KubeClusterResource.getInstance().deleteCustomResources();
        KubeClusterResource.getInstance().deleteNamespaces();
    }
}