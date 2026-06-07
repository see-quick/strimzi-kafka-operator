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
import io.strimzi.api.kafka.model.kafka.KafkaResources;
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
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
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
    description = @Desc("Test suite for measuring CA certificate renewal performance."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default configuration.", expected = "Cluster Operator is deployed and running."),
    },
    labels = {
        @Label(TestDocsLabels.KAFKA)
    }
)
@Tag(PERFORMANCE)
public class CaRenewalPerformance extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CaRenewalPerformance.class);
    private static final String REPORT_DIRECTORY = "cluster-operator";

    private final ClusterOperatorPerformanceReporter reporter = new ClusterOperatorPerformanceReporter();
    private String coScraperPodName;

    @TestDoc(
        description = @Desc("Measures end-to-end time from force-renew annotation on Cluster CA to all broker and controller pods being restarted with the new certificate."),
        steps = {
            @Step(value = "Deploy Kafka cluster with 3 brokers and 3 controllers with metrics enabled.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Start Cluster Operator metrics collection.", expected = "Metrics collection is running."),
            @Step(value = "Capture current Cluster CA certificate value and take pod snapshots.", expected = "Snapshots captured."),
            @Step(value = "Annotate Cluster CA Secret with force-renew=true.", expected = "Secret annotated."),
            @Step(value = "Wait for CA certificate to change and all broker and controller pods to roll.", expected = "New certificate is in the Secret and all pods have restarted."),
            @Step(value = "Stop metrics collection and persist performance data.", expected = "Performance data written to cluster-operator report directory.")
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
    )
    @IsolatedTest
    void testClusterCaRenewal() {
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
            .withScraperPodName(coScraperPodName)
            .withNamespaceName(TestConstants.CO_NAMESPACE)
            .withComponent(ClusterOperatorMetricsComponent.create(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()))
            .build();

        final ClusterOperatorMetricsCollectionScheduler metricsScheduler =
            ClusterOperatorMetricsCollectionScheduler.getInstance(coCollector, "strimzi.io/cluster=" + testStorage.getClusterName());

        metricsScheduler.startCollecting();

        // Capture current cert and pod state
        String clusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName());
        String originalCaCert = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(clusterCaSecretName)
            .get().getData().get("ca.crt");

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        Map<String, String> controllerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getControllerSelector());

        // Annotate Cluster CA secret to force renewal
        long renewalStart = System.currentTimeMillis();
        SecretUtils.annotateSecret(testStorage.getNamespaceName(), clusterCaSecretName,
            ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true");

        LOGGER.info("Waiting for Cluster CA certificate to change and pods to roll");

        // Wait for the cert to actually change
        SecretUtils.waitForCertToChange(testStorage.getNamespaceName(), originalCaCert, clusterCaSecretName, "ca.crt");

        // Wait for both brokers and controllers to roll
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), brokerCount, brokerPods);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), testStorage.getControllerSelector(), controllerCount, controllerPods);

        long renewalTime = System.currentTimeMillis() - renewalStart;

        metricsScheduler.stopCollecting();

        LOGGER.info("Cluster CA renewal (cert change + full rolling restart) took {} ms", renewalTime);

        // Verify cert actually changed
        String newCaCert = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(testStorage.getNamespaceName())
            .withName(clusterCaSecretName)
            .get().getData().get("ca.crt");
        boolean certChanged = !originalCaCert.equals(newCaCert);

        final Map<String, Object> performanceAttributes = new LinkedHashMap<>();
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_BROKER_COUNT, brokerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CONTROLLER_COUNT, controllerCount);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_IN_CA_TYPE, "cluster-ca");
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CA_RENEWAL_TIME, renewalTime);
        performanceAttributes.put(PerformanceConstants.CLUSTER_OPERATOR_OUT_CA_CERT_CHANGED, certChanged);
        performanceAttributes.put(PerformanceConstants.METRICS_HISTORY, metricsScheduler.getMetricsStore());

        try {
            this.reporter.logPerformanceData(testStorage, performanceAttributes,
                REPORT_DIRECTORY + "/" + PerformanceConstants.CLUSTER_OPERATOR_CA_RENEWAL_USE_CASE,
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
