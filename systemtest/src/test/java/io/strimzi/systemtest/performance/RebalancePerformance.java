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
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.metrics.ClusterOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.ClusterOperatorMetricsCollector;
import io.strimzi.systemtest.performance.gather.schedulers.ClusterOperatorMetricsCollectionScheduler;
import io.strimzi.systemtest.performance.report.ClusterOperatorPerformanceReporter;
import io.strimzi.systemtest.performance.report.parser.BasePerformanceMetricsParser;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.PERFORMANCE;

@SuiteDoc(
    description = @Desc("Test suite for measuring KafkaRebalance (Cruise Control) performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class RebalancePerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RebalancePerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();
    private String coScraperPodName;

    @TestDoc(
        description = @Desc("Measures end-to-end rebalance time: from KafkaRebalance CR creation through ProposalReady to Ready state."),
        steps = {
            @Step(value = "Deploy Kafka cluster with Cruise Control and 3 brokers, create topics to give CC data to work with.", expected = "Kafka cluster with Cruise Control deployed, topics created."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Create KafkaRebalance CR and record start time.", expected = "KafkaRebalance CR created."),
            @Step(value = "Wait for ProposalReady state and record proposal time.", expected = "Proposal generated."),
            @Step(value = "Approve the rebalance and wait for Ready state, recording execution time.", expected = "Rebalance completed."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testRebalancePerformance() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerCount = 3;
        final int controllerCount = 3;
        final int topicCount = 50;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), brokerCount).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), controllerCount).build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.cruiseControlMetricsConfigMap(testStorage.getNamespaceName(), testStorage.getClusterName()),
            KafkaTemplates.kafkaWithMetricsAndCruiseControlWithMetrics(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerCount)
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        testStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY,
            KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName());

        // Create topics to give Cruise Control some partitions to work with
        for (int i = 0; i < topicCount; i++) {
            KubeResourceManager.get().createResourceWithoutWait(
                KafkaTopicTemplates.topic(testStorage.getNamespaceName(), "perf-rebalance-topic-" + i, testStorage.getClusterName(), 3, 3)
                    .build()
            );
        }

        // Wait a bit for CC to gather metrics about the new topics
        LOGGER.info("Waiting for Cruise Control to gather enough metrics samples");
        try {
            Thread.sleep(60_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        final ClusterOperatorMetricsCollector coCollector = new ClusterOperatorMetricsCollector.Builder()
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Create KafkaRebalance and measure phases
        long totalStart = System.currentTimeMillis();

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName()).build()
        );

        // Phase 1: Wait for ProposalReady
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(
            testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaRebalanceState.ProposalReady);
        long proposalReadyTime = System.currentTimeMillis() - totalStart;
        LOGGER.info("Proposal ready in {} ms", proposalReadyTime);

        // Phase 2: Approve and wait for Ready (execution)
        long executionStart = System.currentTimeMillis();
        KafkaRebalanceUtils.doRebalancingProcess(testStorage.getNamespaceName(), testStorage.getClusterName());
        long executionTime = System.currentTimeMillis() - executionStart;

        long totalRebalanceTime = System.currentTimeMillis() - totalStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Rebalance total: {} ms (proposal: {} ms, execution: {} ms)", totalRebalanceTime, proposalReadyTime, executionTime);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_TOPIC_COUNT, topicCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_PROPOSAL_READY_TIME, proposalReadyTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_REBALANCE_EXECUTION_TIME, executionTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_TOTAL_REBALANCE_TIME, totalRebalanceTime);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_REBALANCE_USE_CASE,
                TimeHolder.getActualTime(), Environment.PERFORMANCE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    void tearDown() {
        BasePerformanceMetricsParser.main(new String[]{PerformanceConstants.CLUSTER_OPERATOR_PARSER});
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
