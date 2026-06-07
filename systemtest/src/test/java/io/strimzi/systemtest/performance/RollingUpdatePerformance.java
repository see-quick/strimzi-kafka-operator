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
import io.strimzi.api.ResourceAnnotations;
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
import io.strimzi.systemtest.utils.kubeUtils.controllers.StrimziPodSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;

@SuiteDoc(
    description = @Desc("Test suite for measuring Kafka cluster rolling update performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class RollingUpdatePerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RollingUpdatePerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();

    @TestDoc(
        description = @Desc("Measures the wall-clock time for a manual rolling update of brokers and controllers in a 3-broker, 3-controller Kafka cluster."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Take pod snapshots for brokers and controllers.", expected = "Pod snapshots captured."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Annotate broker StrimziPodSet with manual-rolling-update=true and measure time until all broker pods roll.", expected = "All broker pods have been recreated."),
            @Step(value = "Annotate controller StrimziPodSet with manual-rolling-update=true and measure time until all controller pods roll.", expected = "All controller pods have been recreated."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testManualRollingUpdate() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;

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

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(testStorage.getScraperPodName())
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Snapshot pods before rolling update
        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        // --- Roll brokers ---
        long brokerRollStart = System.currentTimeMillis();
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getBrokerComponentName(),
            Collections.singletonMap(ResourceAnnotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerCount, brokerPods);
        long brokerRollTime = System.currentTimeMillis() - brokerRollStart;

        // --- Roll controllers ---
        long controllerRollStart = System.currentTimeMillis();
        StrimziPodSetUtils.annotateStrimziPodSet(testStorage.getNamespaceName(), testStorage.getControllerComponentName(),
            Collections.singletonMap(ResourceAnnotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true"));
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerCount, controllerPods);
        long controllerRollTime = System.currentTimeMillis() - controllerRollStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Broker rolling update took {} ms, controller rolling update took {} ms", brokerRollTime, controllerRollTime);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_BROKER_ROLLING_UPDATE_TIME, brokerRollTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CONTROLLER_ROLLING_UPDATE_TIME, controllerRollTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_TOTAL_ROLLING_UPDATE_TIME, brokerRollTime + controllerRollTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_ROLLING_UPDATE_USE_CASE,
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
    }
}
