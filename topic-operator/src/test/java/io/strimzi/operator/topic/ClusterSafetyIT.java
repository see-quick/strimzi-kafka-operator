/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests that demonstrate cluster safety vulnerabilities in the current UTO implementation.
 *
 * These tests prove that without clusterId/topicId validation:
 * 1. A misconfigured TO can manage topics on the wrong Kafka cluster
 * 2. Deleting a KafkaTopic can delete topics on any connected Kafka cluster
 * 3. Orphaned KafkaTopics can be picked up by a different TO and affect another cluster
 *
 * These tests are designed to FAIL once the cluster safety features are implemented,
 * proving that the new safeguards work correctly.
 */
class ClusterSafetyIT implements TestSeparator {
    private static final Logger LOGGER = LogManager.getLogger(ClusterSafetyIT.class);

    private static final String NAMESPACE = TestUtil.namespaceName(ClusterSafetyIT.class);
    private static final Map<String, String> SELECTOR = Map.of("strimzi.io/cluster", "my-cluster");

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;

    private TopicOperatorMain operator;
    private StrimziKafkaCluster kafkaClusterA;
    private StrimziKafkaCluster kafkaClusterB;
    private Admin adminClientA;
    private Admin adminClientB;

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();
        TestUtil.setupKubeCluster(kubernetesClient, NAMESPACE);
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @AfterEach
    public void afterEach() {
        if (operator != null) {
            operator.stop();
            operator = null;
        }
        if (adminClientA != null) {
            adminClientA.close();
            adminClientA = null;
        }
        if (adminClientB != null) {
            adminClientB.close();
            adminClientB = null;
        }
        if (kafkaClusterA != null) {
            kafkaClusterA.stop();
            kafkaClusterA = null;
        }
        if (kafkaClusterB != null) {
            kafkaClusterB.stop();
            kafkaClusterB = null;
        }
        TestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);
    }

    /**
     * TEST 1: Cluster Misconfiguration
     *
     * Scenario: An operator is accidentally redeployed pointing to a different Kafka cluster
     * (e.g., after infrastructure changes, copy-paste errors, or cluster migration)
     *
     * Current behavior (VULNERABLE):
     * - TO creates topics on Cluster A
     * - TO is restarted pointing to Cluster B (misconfiguration)
     * - TO happily modifies/creates topics on Cluster B
     *
     * Expected behavior (WITH SAFEGUARDS):
     * - TO should detect the cluster mismatch via status.clusterId
     * - TO should refuse to manage topics that belong to a different cluster
     */
    @Test
    @DisplayName("VULNERABILITY: Misconfigured TO manages topics on wrong cluster")
    void shouldDemonstrateClusterMisconfigurationVulnerability() throws Exception {
        // Start two independent Kafka clusters
        kafkaClusterA = startKafkaCluster();
        kafkaClusterB = startKafkaCluster();

        adminClientA = createAdminClient(kafkaClusterA);
        adminClientB = createAdminClient(kafkaClusterB);

        String topicName = "cluster-misconfig-test-topic";

        // Step 1: Start operator pointing to Cluster A and create a topic
        LOGGER.info("Step 1: Starting TO pointing to Cluster A");
        startOperator(kafkaClusterA);

        KafkaTopic kt = createKafkaTopic(topicName);
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).resource(kt).create();

        // Wait for topic to be created on Cluster A
        waitForTopicInKafka(adminClientA, topicName);
        LOGGER.info("Topic '{}' created on Cluster A", topicName);

        // Verify topic does NOT exist on Cluster B
        assertTopicNotExistsInKafka(adminClientB, topicName);
        LOGGER.info("Verified topic '{}' does NOT exist on Cluster B", topicName);

        // Step 2: Stop operator and restart pointing to Cluster B (simulating misconfiguration)
        LOGGER.info("Step 2: Stopping TO and restarting pointing to Cluster B (MISCONFIGURATION)");
        operator.stop();
        operator = null;

        startOperator(kafkaClusterB);

        // Step 3: Trigger reconciliation by updating the KafkaTopic
        LOGGER.info("Step 3: Updating KafkaTopic to trigger reconciliation on wrong cluster");
        KafkaTopic currentKt = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();
        TestUtil.changeTopic(kubernetesClient, currentKt, t -> new KafkaTopicBuilder(t)
            .editSpec()
                .withPartitions(3)  // Change partitions to trigger update
            .endSpec()
            .build());

        // VULNERABILITY: The topic will be created on Cluster B!
        // This should NOT happen with proper clusterId validation
        waitForTopicInKafka(adminClientB, topicName);

        // If we reach here, the vulnerability is confirmed
        LOGGER.warn("VULNERABILITY CONFIRMED: Topic '{}' was created on Cluster B by misconfigured TO!", topicName);
        LOGGER.warn("The TO did not detect that the KafkaTopic was originally managed on a different cluster");

        // This assertion documents the current vulnerable behavior
        // Once clusterId validation is implemented, this test should FAIL
        // (the topic should NOT be created on Cluster B)
        assertTrue(topicExistsInKafka(adminClientB, topicName),
            "VULNERABILITY: Topic was created on wrong cluster without any cluster identity verification");
    }

    /**
     * TEST 2: Accidental Topic Deletion Across Clusters
     *
     * Scenario: After cluster migration or misconfiguration, deleting a KafkaTopic
     * could delete a topic on the wrong cluster that happens to have the same name.
     *
     * Current behavior (VULNERABLE):
     * - Topic "my-topic" exists on both Cluster A and Cluster B
     * - KafkaTopic was originally created for Cluster A
     * - TO is (mis)pointed to Cluster B
     * - Deleting KafkaTopic deletes the topic on Cluster B!
     *
     * Expected behavior (WITH SAFEGUARDS):
     * - TO should verify topicId matches before deletion
     * - TO should refuse to delete topics with mismatched identity
     */
    @Test
    @DisplayName("VULNERABILITY: Deleting KafkaTopic can delete topic on wrong cluster")
    void shouldDemonstrateAccidentalDeletionVulnerability() throws Exception {
        kafkaClusterA = startKafkaCluster();
        kafkaClusterB = startKafkaCluster();

        adminClientA = createAdminClient(kafkaClusterA);
        adminClientB = createAdminClient(kafkaClusterB);

        String topicName = "accidental-delete-test-topic";

        // Step 1: Create topic directly on Cluster B (simulating pre-existing topic)
        LOGGER.info("Step 1: Creating topic '{}' directly on Cluster B", topicName);
        adminClientB.createTopics(Set.of(new NewTopic(topicName, 1, (short) 1))).all().get();
        waitForTopicInKafka(adminClientB, topicName);

        // Step 2: Start operator pointing to Cluster A and create KafkaTopic
        LOGGER.info("Step 2: Starting TO pointing to Cluster A and creating KafkaTopic");
        startOperator(kafkaClusterA);

        KafkaTopic kt = createKafkaTopic(topicName);
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).resource(kt).create();
        waitForTopicInKafka(adminClientA, topicName);
        LOGGER.info("Topic '{}' created on Cluster A via TO", topicName);

        // Step 3: Stop operator and restart pointing to Cluster B
        LOGGER.info("Step 3: Restarting TO pointing to Cluster B (MISCONFIGURATION)");
        operator.stop();
        operator = null;
        startOperator(kafkaClusterB);

        // Step 4: Delete the KafkaTopic
        LOGGER.info("Step 4: Deleting KafkaTopic - this will DELETE topic on Cluster B!");
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).delete();

        // Wait for KafkaTopic resource to be deleted
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName);
        TestUtil.waitUntilCondition(resource, Objects::isNull);

        // VULNERABILITY: The topic on Cluster B is deleted!
        waitForTopicNotInKafka(adminClientB, topicName);

        LOGGER.warn("VULNERABILITY CONFIRMED: Topic on Cluster B was deleted!");
        LOGGER.warn("The TO did not verify that the topic being deleted matches the original topicId");

        // Document the vulnerable behavior
        assertTrue(!topicExistsInKafka(adminClientB, topicName),
            "VULNERABILITY: Topic on wrong cluster was deleted without topicId verification");

        // Note: Topic on Cluster A still exists (orphaned)
        assertTrue(topicExistsInKafka(adminClientA, topicName),
            "Topic on Cluster A still exists (orphaned)");
    }

    /**
     * TEST 3: Orphaned Resources After Cluster Deletion
     *
     * Scenario: After a Kafka cluster is deleted, its KafkaTopic resources remain.
     * If a new cluster is deployed with the same name/labels, the TO could pick up
     * these orphaned resources and create topics on the new cluster.
     *
     * Current behavior (VULNERABLE):
     * - KafkaTopics exist from deleted Cluster A
     * - New Cluster B is deployed
     * - TO picks up orphaned KafkaTopics and creates topics on Cluster B
     *
     * Expected behavior (WITH SAFEGUARDS):
     * - TO should detect clusterId mismatch
     * - TO should report error status instead of creating topics
     */
    @Test
    @DisplayName("VULNERABILITY: Orphaned KafkaTopics affect new cluster")
    void shouldDemonstrateOrphanedResourcesVulnerability() throws Exception {
        // Start initial cluster (Cluster A)
        kafkaClusterA = startKafkaCluster();
        adminClientA = createAdminClient(kafkaClusterA);

        String topicName = "orphaned-resource-test-topic";

        // Step 1: Create topic with TO pointing to Cluster A
        LOGGER.info("Step 1: Creating topic on Cluster A");
        startOperator(kafkaClusterA);

        KafkaTopic kt = createKafkaTopic(topicName);
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).resource(kt).create();
        waitForTopicInKafka(adminClientA, topicName);
        waitForKafkaTopicReady(topicName);
        LOGGER.info("Topic '{}' created and KafkaTopic is ready", topicName);

        // Step 2: Simulate cluster deletion - stop TO and Kafka cluster
        // KafkaTopic resource remains in Kubernetes (orphaned)
        LOGGER.info("Step 2: Simulating Cluster A deletion (TO and Kafka stopped, KafkaTopic remains)");
        operator.stop();
        operator = null;
        adminClientA.close();
        adminClientA = null;
        kafkaClusterA.stop();
        kafkaClusterA = null;

        // Verify KafkaTopic still exists in Kubernetes
        KafkaTopic orphanedKt = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();
        assertNotNull(orphanedKt, "KafkaTopic should still exist after cluster deletion");
        LOGGER.info("KafkaTopic '{}' is now orphaned (cluster deleted)", topicName);

        // Step 3: Deploy new cluster (Cluster B) with same labels
        LOGGER.info("Step 3: Deploying new Cluster B and starting TO");
        kafkaClusterB = startKafkaCluster();
        adminClientB = createAdminClient(kafkaClusterB);

        startOperator(kafkaClusterB);

        // VULNERABILITY: The orphaned KafkaTopic will create a topic on Cluster B!
        waitForTopicInKafka(adminClientB, topicName);

        LOGGER.warn("VULNERABILITY CONFIRMED: Orphaned KafkaTopic created topic on new Cluster B!");
        LOGGER.warn("The TO did not detect that the KafkaTopic was from a different (deleted) cluster");

        assertTrue(topicExistsInKafka(adminClientB, topicName),
            "VULNERABILITY: Orphaned KafkaTopic created topic on wrong cluster without clusterId check");
    }

    // ==================== Helper Methods ====================

    private StrimziKafkaCluster startKafkaCluster() {
        StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withInternalTopicReplicationFactor(1)
            .withAdditionalKafkaConfiguration(Map.of("auto.create.topics.enable", "false"))
            .withSharedNetwork()
            .build();
        cluster.start();
        return cluster;
    }

    private Admin createAdminClient(StrimziKafkaCluster cluster) {
        return Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG, "test-admin-" + System.nanoTime()
        ));
    }

    private void startOperator(StrimziKafkaCluster cluster) {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.RESOURCE_LABELS.key(), Labels.fromMap(SELECTOR).toSelectorString(),
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.CLIENT_ID.key(), ClusterSafetyIT.class.getSimpleName(),
            TopicOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key(), "5000",
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.MAX_QUEUE_SIZE.key(), "100",
            TopicOperatorConfig.MAX_BATCH_SIZE.key(), "100",
            TopicOperatorConfig.MAX_BATCH_LINGER_MS.key(), "10"
        ));

        Admin adminClient = Admin.create(config.adminClientConfig());
        var kubernetesClientOp = new KubernetesClientBuilder()
            .withConfig(kubernetesClient.getConfiguration())
            .build();
        operator = new TopicOperatorMain(config, kubernetesClientOp, adminClient);
        operator.start();
    }

    private KafkaTopic createKafkaTopic(String topicName) {
        return new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .withLabels(SELECTOR)
            .endMetadata()
            .withNewSpec()
                .withTopicName(topicName)
                .withPartitions(1)
                .withReplicas(1)
            .endSpec()
            .build();
    }

    private void waitForTopicInKafka(Admin admin, String topicName) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (topicExistsInKafka(admin, topicName)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new TimeoutException("Topic " + topicName + " was not created in Kafka within timeout");
    }

    private void waitForTopicNotInKafka(Admin admin, String topicName) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (!topicExistsInKafka(admin, topicName)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new TimeoutException("Topic " + topicName + " still exists in Kafka after timeout");
    }

    private boolean topicExistsInKafka(Admin admin, String topicName) {
        try {
            admin.describeTopics(Set.of(topicName)).topicNameValues().get(topicName).get();
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void assertTopicNotExistsInKafka(Admin admin, String topicName) {
        if (topicExistsInKafka(admin, topicName)) {
            fail("Expected topic " + topicName + " to NOT exist in Kafka");
        }
    }

    private void waitForKafkaTopicReady(String topicName) {
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient)
            .inNamespace(NAMESPACE)
            .withName(topicName);
        TestUtil.waitUntilCondition(resource, kt ->
            kt != null &&
            kt.getStatus() != null &&
            kt.getStatus().getConditions() != null &&
            kt.getStatus().getConditions().stream()
                .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()))
        );
    }
}