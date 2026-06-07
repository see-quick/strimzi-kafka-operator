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
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;
import static io.strimzi.systemtest.TestTags.SCALABILITY;

@SuiteDoc(
    description = @Desc("Test suite for measuring Kafka node pool scale-up and scale-down performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
@Tag(SCALABILITY)
public class NodePoolScalingPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(NodePoolScalingPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();
    private String coScraperPodName;

    @TestDoc(
        description = @Desc("Measures the wall-clock time to scale a broker node pool from 3 to 5 and back to 3 replicas."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Scale broker pool from 3 to 5 replicas via KafkaNodePool CR patch and measure time until all 5 pods are ready.", expected = "5 broker pods are running and ready."),
            @Step(value = "Scale broker pool from 5 to 3 replicas via KafkaNodePool CR patch and measure time until exactly 3 pods are ready.", expected = "3 broker pods are running and ready."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testNodePoolScaleUpDown() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int initialBrokers = 3;
        final int scaledBrokers = 5;
        final int controllerCount = 3;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialBrokers).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), initialBrokers)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // --- Scale up: 3 -> 5 ---
        long scaleUpStart = System.currentTimeMillis();
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), scaledBrokers);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), scaledBrokers);
        long scaleUpTime = System.currentTimeMillis() - scaleUpStart;

        LOGGER.info("Scale up from {} to {} took {} ms", initialBrokers, scaledBrokers, scaleUpTime);

        // --- Scale down: 5 -> 3 ---
        long scaleDownStart = System.currentTimeMillis();
        KafkaNodePoolUtils.scaleKafkaNodePool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), initialBrokers);
        RollingUpdateUtils.waitForComponentScaleUpOrDown(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), initialBrokers);
        long scaleDownTime = System.currentTimeMillis() - scaleDownStart;

        LOGGER.info("Scale down from {} to {} took {} ms", scaledBrokers, initialBrokers, scaleDownTime);

        metricsScheduler.stopCollecting();

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, initialBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_INITIAL_BROKER_COUNT, initialBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_SCALED_BROKER_COUNT, scaledBrokers);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_SCALE_UP_TIME, scaleUpTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_SCALE_DOWN_TIME, scaleDownTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_NODE_POOL_SCALING_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
