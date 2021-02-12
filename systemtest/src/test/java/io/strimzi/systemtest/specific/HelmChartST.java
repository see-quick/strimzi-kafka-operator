/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.operator.HelmResource;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.HELM;

@Tag(HELM)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";

    @ParallelTest
    void testDeployKafkaClusterViaHelmChart(ExtensionContext extensionContext) {
        String clusterName = mapTestWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapTestWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 3);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(NAMESPACE);
        resourceManager.createResource(extensionContext, HelmResource.clusterOperator());
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        cluster.deleteNamespaces();
    }
}
