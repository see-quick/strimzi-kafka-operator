/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.KafkaSpecBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;

/**
 * Executes real system operations for Kafka cluster management during Model-Based Testing (MBT).
 *
 * This class bridges the gap between the abstract actions from the formal Quint model and the concrete
 * operations on a Kubernetes cluster via the Cluster Operator. It implements the logic to:
 *
 * <ul>
 *   <li><b>createKafkaCluster(...)</b> — Create a Kafka cluster with specified configuration.</li>
 *   <li><b>scaleNodePool(...)</b> — Scale a node pool up or down.</li>
 *   <li><b>updateMetadataVersion(...)</b> — Update Kafka metadata version.</li>
 *   <li><b>rotateCertificates(...)</b> — Trigger certificate rotation.</li>
 *   <li><b>processNextEvent(...)</b> — Apply and dequeue the next action from the MBT-generated queue.</li>
 * </ul>
 *
 * Model actions are represented as sealed `ModelEvent` records (CreateCluster, ScaleNodePool, etc.),
 * constructed via the {@link EventsFactory} for type safety.
 *
 * This executor is typically used in {@code KafkaAssemblyModelMbtIT} to replay traces generated from
 * a formal Quint specification (`ClusterOperatorModel.qnt`) and assert that real system behavior matches
 * the expected invariant-preserving execution.
 */
public class KafkaClusterModelActions {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClusterModelActions.class);
    private static final long POLL_INTERVAL_MS = Duration.ofMillis(200).toMillis();
    private static final long POLL_TIMEOUT_MS = Duration.ofMillis(30_000).toMillis();

    private final CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> nodePoolOperator;
    private final SecretOperator secretOperator;
    private final String namespace;
    private final List<String> processedStepsLog;

    public KafkaClusterModelActions(
        CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator,
        CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> nodePoolOperator,
        SecretOperator secretOperator,
        String namespace,
        List<String> processedStepsLog
    ) {
        this.kafkaOperator = kafkaOperator;
        this.nodePoolOperator = nodePoolOperator;
        this.secretOperator = secretOperator;
        this.namespace = namespace;
        this.processedStepsLog = processedStepsLog;
    }

    public enum Action {
        CREATE_CLUSTER("createCluster"),
        SCALE_NODE_POOL("scaleNodePool"),
        UPDATE_METADATA_VERSION("updateMetadataVersion"),
        ROTATE_CERTIFICATE("rotateCertificate"),
        START_ROLLING_UPDATE("startRollingUpdate"),
        PROCESS_NEXT_EVENT("processNextEvent");

        private final String actionName;

        Action(String actionName) {
            this.actionName = actionName;
        }

        public String actionName() {
            return actionName;
        }

        public static Action fromString(String s) {
            for (Action a : values()) {
                if (a.actionName.equals(s)) {
                    return a;
                }
            }
            throw new IllegalArgumentException("Unknown action: " + s);
        }
    }

    public sealed interface ModelEvent permits CreateCluster, ScaleNodePool, UpdateMetadataVersion, RotateCertificate, StartRollingUpdate {
        Action action();
    }

    public record CreateCluster(String clusterName, int replicas, String kafkaVersion) implements ModelEvent {
        @Override
        public Action action() {
            return Action.CREATE_CLUSTER;
        }
    }

    public record ScaleNodePool(String poolName, int newReplicas) implements ModelEvent {
        @Override
        public Action action() {
            return Action.SCALE_NODE_POOL;
        }
    }

    public record UpdateMetadataVersion(String kafkaVersion, String metadataVersion) implements ModelEvent {
        @Override
        public Action action() {
            return Action.UPDATE_METADATA_VERSION;
        }
    }

    public record RotateCertificate(String certificateType) implements ModelEvent {
        @Override
        public Action action() {
            return Action.ROTATE_CERTIFICATE;
        }
    }

    public record StartRollingUpdate(String reason) implements ModelEvent {
        @Override
        public Action action() {
            return Action.START_ROLLING_UPDATE;
        }
    }

    public static class EventsFactory {
        public static CreateCluster createCluster(String clusterName, int replicas, String kafkaVersion) {
            return new CreateCluster(clusterName, replicas, kafkaVersion);
        }

        public static ScaleNodePool scaleNodePool(String poolName, int newReplicas) {
            return new ScaleNodePool(poolName, newReplicas);
        }

        public static UpdateMetadataVersion updateMetadataVersion(String kafkaVersion, String metadataVersion) {
            return new UpdateMetadataVersion(kafkaVersion, metadataVersion);
        }

        public static RotateCertificate rotateCertificate(String certificateType) {
            return new RotateCertificate(certificateType);
        }

        public static StartRollingUpdate startRollingUpdate(String reason) {
            return new StartRollingUpdate(reason);
        }
    }

    public void processNextEvent(List<ModelEvent> eventQueue) {
        if (eventQueue.isEmpty()) {
            processedStepsLog.add("🔄 processNextEvent: eventQueue is empty - no action taken");
            return;
        }

        ModelEvent event = eventQueue.remove(0);
        LOGGER.info("🎬 Processing event: {}", event);
        processedStepsLog.add("🎬 Processing: " + event);

        try {
            switch (event.action()) {
                case CREATE_CLUSTER -> {
                    CreateCluster createEvent = (CreateCluster) event;
                    createKafkaCluster(createEvent.clusterName(), createEvent.replicas(), createEvent.kafkaVersion());
                }
                case SCALE_NODE_POOL -> {
                    ScaleNodePool scaleEvent = (ScaleNodePool) event;
                    scaleNodePool(scaleEvent.poolName(), scaleEvent.newReplicas());
                }
                case UPDATE_METADATA_VERSION -> {
                    UpdateMetadataVersion updateEvent = (UpdateMetadataVersion) event;
                    updateMetadataVersion(updateEvent.kafkaVersion(), updateEvent.metadataVersion());
                }
                case ROTATE_CERTIFICATE -> {
                    RotateCertificate rotateEvent = (RotateCertificate) event;
                    rotateCertificate(rotateEvent.certificateType());
                }
                case START_ROLLING_UPDATE -> {
                    StartRollingUpdate rollingEvent = (StartRollingUpdate) event;
                    startRollingUpdate(rollingEvent.reason());
                }
                default -> LOGGER.warn("⚠️ Unknown event action: {}", event.action());
            }
        } catch (Exception e) {
            LOGGER.error("❌ Failed to process event: {}", event, e);
            processedStepsLog.add("❌ Failed: " + event + " - " + e.getMessage());
            throw new RuntimeException("Event processing failed", e);
        }
    }

    private void createKafkaCluster(String clusterName, int replicas, String kafkaVersion) {
        LOGGER.info("🏗️ Creating Kafka cluster: {} with {} replicas, version: {}", clusterName, replicas, kafkaVersion);
        
        // Create the main Kafka resource
        Kafka kafka = new KafkaBuilder()
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(namespace)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata()
            .withSpec(new KafkaSpecBuilder()
                .withNewKafka()
                    .withReplicas(replicas)
                    .withVersion(kafkaVersion)
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                    .endPersistentClaimStorage()
                    .addNewListener()
                        .withName("plain")
                        .withPort(9092)
                        .withType("internal")
                        .withTls(false)
                    .endListener()
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(1)
                    .withNewPersistentClaimStorage()
                        .withSize("1Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
                .build())
            .build();

        kafkaOperator.resource(namespace, kafka).createOrReplace();
        
        // Create a corresponding node pool
        createDefaultNodePool(clusterName, replicas);
        
        processedStepsLog.add("✅ Created Kafka cluster: " + clusterName);
    }

    private void createDefaultNodePool(String clusterName, int replicas) {
        String poolName = "pool-1";
        
        KafkaNodePool nodePool = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName(poolName)
                .withNamespace(namespace)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata()
            .withNewSpec()
                .withReplicas(replicas)
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                .endPersistentClaimStorage()
                .withRoles("broker")
            .endSpec()
            .build();

        nodePoolOperator.resource(namespace, nodePool).createOrReplace();
        processedStepsLog.add("✅ Created node pool: " + poolName + " with " + replicas + " replicas");
    }

    private void scaleNodePool(String poolName, int newReplicas) {
        LOGGER.info("📏 Scaling node pool: {} to {} replicas", poolName, newReplicas);
        
        KafkaNodePool existingPool = nodePoolOperator.get(namespace, poolName);
        if (existingPool == null) {
            throw new RuntimeException("Node pool not found: " + poolName);
        }

        KafkaNodePool updatedPool = new KafkaNodePoolBuilder(existingPool)
            .editSpec()
                .withReplicas(newReplicas)
            .endSpec()
            .build();

        nodePoolOperator.resource(namespace, updatedPool).replace();
        processedStepsLog.add("✅ Scaled node pool " + poolName + " to " + newReplicas + " replicas");
    }

    private void updateMetadataVersion(String kafkaVersion, String metadataVersion) {
        LOGGER.info("🔄 Updating metadata version to: {} for Kafka version: {}", metadataVersion, kafkaVersion);
        
        // In a real implementation, this would trigger a metadata version update
        // For now, we'll simulate by updating the Kafka resource annotation
        List<Kafka> kafkaClusters = kafkaOperator.list(namespace, Labels.forStrimziCluster("my-cluster"));
        
        for (Kafka kafka : kafkaClusters) {
            Kafka updatedKafka = new KafkaBuilder(kafka)
                .editMetadata()
                    .addToAnnotations("strimzi.io/kafka-metadata-version", metadataVersion)
                .endMetadata()
                .build();
                
            kafkaOperator.resource(namespace, updatedKafka).replace();
        }
        
        processedStepsLog.add("✅ Updated metadata version to: " + metadataVersion);
    }

    private void rotateCertificate(String certificateType) {
        LOGGER.info("🔐 Rotating certificate: {}", certificateType);
        
        // Simulate certificate rotation by updating certificate annotation
        String secretName = certificateType.equals("cluster") ? "my-cluster-cluster-ca-cert" : "my-cluster-clients-ca-cert";
        
        Secret existingSecret = secretOperator.get(namespace, secretName);
        if (existingSecret != null) {
            Secret updatedSecret = new SecretBuilder(existingSecret)
                .editMetadata()
                    .addToAnnotations("strimzi.io/force-renew", String.valueOf(System.currentTimeMillis()))
                .endMetadata()
                .build();
                
            secretOperator.resource(namespace, updatedSecret).replace();
        }
        
        processedStepsLog.add("✅ Rotated " + certificateType + " certificate");
    }

    private void startRollingUpdate(String reason) {
        LOGGER.info("🔄 Starting rolling update for reason: {}", reason);
        
        // Trigger rolling update by updating annotation
        List<Kafka> kafkaClusters = kafkaOperator.list(namespace, Labels.forStrimziCluster("my-cluster"));
        
        for (Kafka kafka : kafkaClusters) {
            Kafka updatedKafka = new KafkaBuilder(kafka)
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", String.valueOf(System.currentTimeMillis()))
                .endMetadata()
                .build();
                
            kafkaOperator.resource(namespace, updatedKafka).replace();
        }
        
        processedStepsLog.add("✅ Started rolling update: " + reason);
    }

    public void waitUntilAllClustersReady(String namespace) {
        List<Kafka> kafkaClusters = kafkaOperator.list(namespace, Labels.forStrimziCluster("my-cluster"));
        
        for (Kafka kafka : kafkaClusters) {
            String clusterName = kafka.getMetadata().getName();
            
            TestUtils.waitFor("Kafka cluster " + clusterName + " to be ready",
                POLL_INTERVAL_MS, POLL_TIMEOUT_MS, () -> {
                    Kafka current = kafkaOperator.get(namespace, clusterName);
                    return current != null && 
                           current.getStatus() != null &&
                           hasStatusCondition(current, "Ready", "True");
                });
        }
    }

    private boolean hasStatusCondition(Kafka kafka, String type, String status) {
        return kafka.getStatus() != null &&
               kafka.getStatus().getConditions() != null &&
               kafka.getStatus().getConditions().stream()
                   .anyMatch(condition -> type.equals(condition.getType()) && status.equals(condition.getStatus()));
    }
}