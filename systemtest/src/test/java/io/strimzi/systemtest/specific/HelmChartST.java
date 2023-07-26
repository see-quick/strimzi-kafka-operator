/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.HELM;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(HELM)
@Tag(REGRESSION)
class HelmChartST extends AbstractST {

    @IsolatedTest
    void testStrimziComponentsViaHelmChart(ExtensionContext extensionContext) {
        final TestStorage ts = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        // Deploy Kafka and wait for readiness
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(ts.getClusterName(), 3).build());

        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(ts.getClusterName(), ts.getTopicName(), Constants.TEST_SUITE_NAMESPACE).build(),
            // Deploy KafkaConnect and wait for readiness
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(ts.getClusterName(), Constants.TEST_SUITE_NAMESPACE, 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .build(),
            // Deploy KafkaBridge (different image than Kafka) and wait for readiness
            KafkaBridgeTemplates.kafkaBridge(ts.getClusterName(), KafkaResources.plainBootstrapAddress(ts.getClusterName()), 1).build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(ts.getClusterName()).build());
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator = clusterOperator.defaultInstallation(extensionContext)
            .createInstallation()
            // run always Helm installation
            .runHelmInstallation();

        cluster.setNamespace(Constants.TEST_SUITE_NAMESPACE);
    }
}
