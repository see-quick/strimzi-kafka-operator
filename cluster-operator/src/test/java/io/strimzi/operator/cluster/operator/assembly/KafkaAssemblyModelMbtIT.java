/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

/**
 * Model-Based Test (MBT) for the Strimzi Cluster Operator.
 *
 * This test replays formal execution traces (JSON files) generated from a Quint specification
 * (`ClusterOperatorModel.qnt`) and validates that the real Java Cluster Operator behaves as the model expects.
 *
 * Each trace describes a sequence of actions (createCluster, scaleNodePool, updateMetadataVersion, 
 * rotateCertificate, startRollingUpdate, processNextEvent).
 * - Actions are queued until `processNextEvent` is explicitly requested, mimicking the model's event queue.
 * - After each step, invariants (cluster consistency, certificate validity, node ID uniqueness) are checked against the real system.
 *
 * Components under test include:
 * - Kafka cluster creation and scaling
 * - Node pool management and scaling operations
 * - Rolling updates and pod management
 * - Certificate rotation and management
 * - Metadata version upgrades
 *
 * This test ensures that the real implementation honors the same invariants and properties as the Quint model.
 */
public class KafkaAssemblyModelMbtIT {
    private static final Logger LOGGER = LogManager.getLogger(KafkaAssemblyModelMbtIT.class);

    private static MockKube3 mockKube;
    private static StrimziKafkaCluster kafkaCluster;
    private static Vertx vertx;

    private CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> nodePoolOperator;
    private StrimziPodSetOperator podSetOperator;
    private PodOperator podOperator;
    private SecretOperator secretOperator;
    private String namespace;

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Trace {
        public List<Map<String, Object>> states;
    }

    static Stream<String> testCaseProvider() {
        try {
            File dir = new File(KafkaAssemblyModelMbtIT.class.getResource("/specification/traces/").toURI());
            return Stream.of(dir.listFiles())
                .filter(f -> f.getName().endsWith(".json"))
                .map(f -> "/specification/traces/" + f.getName());
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    @ParameterizedTest
    @MethodSource("testCaseProvider")
    public void testKafkaAssemblyOperator(String tracePath) throws Exception {
        final List<String> mbtTimeline = new ArrayList<>();
        final Trace testCase = parseTestCase(tracePath);
        final TestParameters parameters = extractTestParameters(testCase);
        
        logTestParameters(tracePath, parameters);
        
        final KafkaClusterModelActions actions = setupTestEnvironment(mbtTimeline);
        final List<KafkaClusterModelActions.ModelEvent> eventQueue = new ArrayList<>();

        try {
            executeTestTrace(testCase, parameters, actions, eventQueue);
        } catch (AssertionError e) {
            LOGGER.error("❌ Invariant failure after processNextEvent. Printing timeline of events for trace: {}", tracePath);
            mbtTimeline.forEach(LOGGER::error);
            throw e;
        } finally {
            LOGGER.info("ℹ️ MBT test trace for {}. Timeline of actions:", tracePath);
            mbtTimeline.forEach(LOGGER::debug);
        }
    }

    private Trace parseTestCase(String tracePath) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final InputStream input = getClass().getResourceAsStream(tracePath);
        return mapper.readValue(input, Trace.class);
    }

    private TestParameters extractTestParameters(Trace testCase) {
        final Map<String, Object> parameters = (Map<String, Object>) testCase.states.get(0).get("parameters");
        return new TestParameters(
            (Boolean) parameters.get("enableRollingUpdates"),
            (Boolean) parameters.get("enableCertRotation"),
            (Boolean) parameters.get("enableAutoRebalance"),
            Integer.parseInt((String) ((Map<String, Object>) parameters.get("maxProcessedEvents")).get("#bigint"))
        );
    }

    private void logTestParameters(String tracePath, TestParameters parameters) {
        LOGGER.info("📋 STARTING TEST CASE: {}", tracePath);
        LOGGER.info("====================");
        LOGGER.info("1️⃣ PARAMETER enableRollingUpdates = {}", parameters.enableRollingUpdates);
        LOGGER.info("2️⃣ PARAMETER enableCertRotation = {}", parameters.enableCertRotation);
        LOGGER.info("3️⃣ PARAMETER enableAutoRebalance = {}", parameters.enableAutoRebalance);
        LOGGER.info("4️⃣ PARAMETER maxProcessedEvents = {}", parameters.maxProcessedEvents);
    }

    private KafkaClusterModelActions setupTestEnvironment(List<String> mbtTimeline) {
        return new KafkaClusterModelActions(
            kafkaOperator, nodePoolOperator, secretOperator, namespace, mbtTimeline);
    }

    private void executeTestTrace(Trace testCase, TestParameters parameters, 
                                 KafkaClusterModelActions actions, 
                                 List<KafkaClusterModelActions.ModelEvent> eventQueue) {
        for (int i = 0; i < testCase.states.size(); i++) {
            final Map<String, Object> state = testCase.states.get(i);
            final Map<String, Object> globalState = (Map<String, Object>) state.get("globalState");
            final int processedEvents = Integer.parseInt(
                String.valueOf(((Map<String, Object>) globalState.get("processedEvents")).get("#bigint"))
            );

            if (processedEvents >= parameters.maxProcessedEvents) {
                LOGGER.info("🍀 Reached maxProcessedEvents={} at step {} — finishing early.", parameters.maxProcessedEvents, i);
                break;
            }

            processTestStep(i, state, actions, eventQueue);
        }
    }

    private void processTestStep(int stepIndex, Map<String, Object> state, 
                                KafkaClusterModelActions actions, 
                                List<KafkaClusterModelActions.ModelEvent> eventQueue) {
        final String action = (String) state.get("mbt::actionTaken");
        final Map<String, Object> nondet = (Map<String, Object>) state.get("mbt::nondetPicks");
        
        final StepParameters stepParams = extractStepParameters(nondet);
        logStepInfo(stepIndex, action, stepParams);

        if (stepIndex > 0) {
            executeStepAction(action, stepParams, actions, eventQueue);
        }
    }

    private void executeStepAction(String action, StepParameters stepParams, 
                                  KafkaClusterModelActions actions, 
                                  List<KafkaClusterModelActions.ModelEvent> eventQueue) {
        final KafkaClusterModelActions.Action act = KafkaClusterModelActions.Action.fromString(action);
        switch (act) {
            case CREATE_CLUSTER ->
                eventQueue.add(KafkaClusterModelActions.EventsFactory.createCluster(
                    stepParams.clusterName != null ? stepParams.clusterName : "my-cluster",
                    stepParams.replicas != null ? stepParams.replicas : 3,
                    stepParams.kafkaVersion != null ? stepParams.kafkaVersion : "3.0.0"));
            case SCALE_NODE_POOL ->
                eventQueue.add(KafkaClusterModelActions.EventsFactory.scaleNodePool(
                    stepParams.poolName != null ? stepParams.poolName : "pool-1",
                    stepParams.replicas != null ? stepParams.replicas : 3));
            case UPDATE_METADATA_VERSION ->
                eventQueue.add(KafkaClusterModelActions.EventsFactory.updateMetadataVersion(
                    stepParams.kafkaVersion != null ? stepParams.kafkaVersion : "3.0.0",
                    stepParams.metadataVersion != null ? stepParams.metadataVersion : "3.0"));
            case ROTATE_CERTIFICATE ->
                eventQueue.add(KafkaClusterModelActions.EventsFactory.rotateCertificate(
                    stepParams.certificateType != null ? stepParams.certificateType : "cluster"));
            case START_ROLLING_UPDATE ->
                eventQueue.add(KafkaClusterModelActions.EventsFactory.startRollingUpdate(
                    stepParams.rollingUpdateReason != null ? stepParams.rollingUpdateReason : "manual"));
            case PROCESS_NEXT_EVENT -> {
                actions.processNextEvent(eventQueue);
                actions.waitUntilAllClustersReady(namespace);
                checkAllInvariants();
            }
            default -> { /* no-op */ }
        }
    }

    private void checkAllInvariants() {
        final ClusterInvariantChecker invariants = new ClusterInvariantChecker(
            kafkaOperator, nodePoolOperator, podSetOperator, podOperator, secretOperator);
        
        invariants.assertClusterIdConsistency(namespace);
        invariants.assertMetadataVersionMonotonicity(namespace);
        invariants.assertMinimumReadyReplicas(namespace);
        invariants.assertCertificateValidityOverlap(namespace);
        invariants.assertNodeIdUniqueness(namespace);
        invariants.assertAutoRebalanceConsistency(namespace);
        invariants.assertRollingUpdateConsistency(namespace);
    }

    private StepParameters extractStepParameters(Map<String, Object> nondet) {
        if (nondet == null) {
            return new StepParameters(null, null, null, null, null, null, null);
        }
        
        return new StepParameters(
            getOptionalValue(nondet, "clusterName", String.class),
            getOptionalValue(nondet, "poolName", String.class),
            getOptionalValue(nondet, "replicas", Integer.class),
            getOptionalValue(nondet, "kafkaVersion", String.class),
            getOptionalValue(nondet, "metadataVersion", String.class),
            getOptionalValue(nondet, "certificateType", String.class),
            getOptionalValue(nondet, "rollingUpdateReason", String.class)
        );
    }

    private void logStepInfo(int stepIndex, String action, StepParameters stepParams) {
        final String stepInfo = String.format("""
            ⊧ MBT: [%d] %s action='%s'
                ├─ clusterName='%s'
                ├─ poolName='%s'
                ├─ replicas='%s'
                ├─ kafkaVersion='%s'
                ├─ metadataVersion='%s'
                ├─ certificateType='%s'
                └─ rollingUpdateReason='%s'
            """,
            stepIndex,
            "processNextEvent".equals(action) ? "Executing" : "Queueing",
            action,
            stepParams.clusterName,
            stepParams.poolName,
            stepParams.replicas,
            stepParams.kafkaVersion,
            stepParams.metadataVersion,
            stepParams.certificateType,
            stepParams.rollingUpdateReason
        );
        LOGGER.debug(stepInfo);
    }

    private record TestParameters(Boolean enableRollingUpdates, Boolean enableCertRotation,
                                  Boolean enableAutoRebalance, Integer maxProcessedEvents) { }

    private record StepParameters(String clusterName, String poolName, Integer replicas,
                                  String kafkaVersion, String metadataVersion,
                                  String certificateType, String rollingUpdateReason) { }

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Skip Kafka cluster setup for now as cluster operator doesn't directly connect to Kafka
        // In a full integration test, you might want to set up a test Kafka cluster
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaCrd()
            .withKafkaNodePoolCrd()
            .withStrimziPodSetCrd()
            .withDeletionController()
            .build();
        mockKube.start();

        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);

        // Wait until the namespace is truly gone
        mockKube.prepareNamespace(namespace);

        final KubernetesClient client = mockKube.client();
        kafkaOperator = new CrdOperator<>(ForkJoinPool.commonPool(), client, Kafka.class, KafkaList.class, "Kafka");
        nodePoolOperator = new CrdOperator<>(ForkJoinPool.commonPool(), client, KafkaNodePool.class, KafkaNodePoolList.class, "KafkaNodePool");
        secretOperator = new SecretOperator(ForkJoinPool.commonPool(), client);
        podSetOperator = new StrimziPodSetOperator(ForkJoinPool.commonPool(), client);
        podOperator = new PodOperator(ForkJoinPool.commonPool(), client);

        // Create dummy cluster CA and clients CA certificates required by the operator
        createDummyCertificates();
    }

    @AfterEach
    void afterEach() {
        if (mockKube != null) {
            mockKube.stop();
        }
    }

    @AfterAll
    public static void tearDown() {
        if (vertx != null) {
            vertx.close();
        }
    }

    private void createDummyCertificates() {
        // Create dummy cluster CA certificate
        final Secret clusterCaCert = new SecretBuilder()
            .withNewMetadata()
                .withName("my-cluster-cluster-ca-cert")
                .withNamespace(namespace)
                .addToAnnotations("strimzi.io/ca-cert-generation", "1")
            .endMetadata()
            .withData(Map.of("ca.crt", "ZHVtbXk=")) // "dummy" base64-encoded
            .build();

        // Create dummy clients CA certificate
        final Secret clientsCaCert = new SecretBuilder()
            .withNewMetadata()
                .withName("my-cluster-clients-ca-cert")
                .withNamespace(namespace)
                .addToAnnotations("strimzi.io/ca-cert-generation", "1")
            .endMetadata()
            .withData(Map.of("ca.crt", "ZHVtbXk=")) // "dummy" base64-encoded
            .build();

        secretOperator.resource(namespace, clusterCaCert).createOrReplace();
        secretOperator.resource(namespace, clientsCaCert).createOrReplace();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getOptionalValue(Map<String, Object> map, String key, Class<T> type) {
        Object value = map.get(key);
        if (value != null && type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }
}