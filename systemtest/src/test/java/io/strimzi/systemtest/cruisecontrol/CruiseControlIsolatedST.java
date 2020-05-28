/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CruiseControlIsolatedST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlIsolatedST.class);
    private static final String NAMESPACE = "cruise-control-isolated-test";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2


    @Test
    void testManuallyCreateMetricsReporterTopic() {
        KafkaResource.kafkaWithCruiseControlWithoutWaitAutoCreateTopicsDisable(CLUSTER_NAME, 3, 3);

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        PodUtils.waitUntilPodIsInCrashLoopBackOff(PodUtils.getFirstPodNameContaining("cruise-control"));

        LOGGER.info("Verifying that samples topics are not present because of " +
            "'Cruise Control cannot find partitions for the metrics reporter that topic matches strimzi.cruisecontrol.metrics in the target cluster'");

        assertThrows(WaitException.class, CruiseControlUtils::verifyThatCruiseControlSamplesTopicsArePresent);

        LOGGER.info("Verifying that metrics reporter topic is not present because of selected config 'auto.create.topics.enable=false'");

        assertThrows(WaitException.class, CruiseControlUtils::verifyThatKafkaCruiseControlMetricReporterTopicIsPresent);

        LOGGER.info("Creating samples topics {},{}", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);

        KafkaTopicResource.topic(CLUSTER_NAME, CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC)
            .editSpec()
                .withPartitions(32)
                .withReplicas(2)
                .addToConfig("cleanup.policy", "delete")
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC)
            .editSpec()
                .withPartitions(32)
                .withReplicas(2)
                .addToConfig("cleanup.policy", "delete")
            .endSpec()
            .done();

        CruiseControlUtils.verifyThatCruiseControlSamplesTopicsArePresent();

        LOGGER.info("Verifying that metrics reporter topic is not present because of selected config 'auto.create.topics.enable=false'");

        assertThrows(WaitException.class, CruiseControlUtils::verifyThatKafkaCruiseControlMetricReporterTopicIsPresent);

        // Since log compaction may remove records needed by Cruise Control, all topics created by Cruise Control must
        // be configured with cleanup.policy=delete to disable log compaction.
        // More in docs 8.5.2. Topic creation and configuration
        KafkaTopicResource.topic(CLUSTER_NAME, CRUISE_CONTROL_METRICS_TOPIC)
            .editSpec()
                .addToConfig("cleanup.policy", "delete")
            .endSpec()
            .done();

        CruiseControlUtils.verifyThatKafkaCruiseControlMetricReporterTopicIsPresent();
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
