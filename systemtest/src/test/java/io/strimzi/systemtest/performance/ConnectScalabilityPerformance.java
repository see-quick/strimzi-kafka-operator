/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring KafkaConnect connector scalability."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class ConnectScalabilityPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ConnectScalabilityPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();
    private String coScraperPodName;

    @TestDoc(
        description = @Desc("Measures how long it takes to deploy and reconcile increasing numbers of KafkaConnector CRs (10, 25, 50)."),
        steps = {
            @Step(value = "Deploy Kafka cluster and KafkaConnect with file sink plugin.", expected = "Kafka and KafkaConnect clusters are deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "For each connector count (10, 25, 50): create N KafkaConnector CRs and measure time until all reach Ready state.", expected = "All connectors are Ready. Time recorded for each batch."),
            @Step(value = "Clean up connectors between iterations.", expected = "All connectors deleted."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testConnectorScalability() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;
        final List<Integer> connectorCounts = List.of(10, 25, 50);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        // Deploy KafkaConnect with the file sink plugin and annotations for connectors
        KubeResourceManager.get().createResourceWithWait(
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .build()
        );

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        connectorCounts.forEach(numberOfConnectors -> {
            long allConnectorsReadyTime = 0;
            try {
                // Create N connectors without waiting
                long createStart = System.currentTimeMillis();
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    KubeResourceManager.get().createResourceWithoutWait(
                        KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), connectorName, testStorage.getClusterName(), 1)
                            .editSpec()
                                .addToConfig("file", "/opt/kafka/LICENSE")
                                .addToConfig("topic", "perf-topic-" + i)
                            .endSpec()
                            .build()
                    );
                }

                // Wait for all connectors to be ready
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    var connector = CrdClients.kafkaConnectorClient()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(connectorName)
                        .get();
                    KubeResourceManager.get().waitResourceCondition(connector, ResourceConditions.resourceIsReady());
                }

                allConnectorsReadyTime = System.currentTimeMillis() - createStart;
                LOGGER.info("{} connectors reached Ready state in {} ms", numberOfConnectors, allConnectorsReadyTime);
            } finally {
                // Cleanup connectors
                for (int i = 0; i < numberOfConnectors; i++) {
                    String connectorName = "perf-connector-" + i;
                    CrdClients.kafkaConnectorClient()
                        .inNamespace(testStorage.getNamespaceName())
                        .withName(connectorName)
                        .delete();
                }

                final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONNECTOR_COUNT, numberOfConnectors);
                performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_ALL_CONNECTORS_READY_TIME, allConnectorsReadyTime);

                try {
                    this.reporter.logPerformanceData(testStorage, performanceAttributes,
                        REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_CONNECT_SCALABILITY_USE_CASE,
                        TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        metricsScheduler.stopCollecting();
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .install();

        NetworkPolicyUtils.allowNetworkPolicySettingsForClusterOperator(TestConstants.CO_NAMESPACE);

        KubeResourceManager.get().createResourceWithWait(
            ScraperTemplates.scraperPod(TestConstants.CO_NAMESPACE, "co-perf-scraper").build()
        );

        coScraperPodName = KubeResourceManager.get().kubeClient()
            .listPodsByPrefixInName(TestConstants.CO_NAMESPACE, "co-perf-scraper")
            .get(0).getMetadata().getName();
    }
}
