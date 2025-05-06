package io.strimzi.operator.user;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Executes real system operations for KafkaUser management during Model-Based Testing (MBT).
 *
 * This class bridges the gap between the abstract actions from the formal Quint model and the concrete
 * operations on a Kubernetes + Kafka cluster via the User Operator. It implements the logic to:
 *
 * <ul>
 *   <li><b>createKafkaUser(...)</b> — Create a KafkaUser with specified auth, quotas, and ACLs.</li>
 *   <li><b>updateKafkaUser(...)</b> — Randomly mutate an existing KafkaUser to force reconciliation.</li>
 *   <li><b>deleteKafkaUser(...)</b> — Delete a KafkaUser and ensure its Secret is also cleaned up.</li>
 *   <li><b>processNextEvent(...)</b> — Apply and dequeue the next action from the MBT-generated queue.</li>
 * </ul>
 *
 * Model actions are represented as sealed `ModelEvent` records (Create, Update, Delete),
 * constructed via the {@link EventsFactory} for type safety.
 *
 * This executor is typically used in {@code UserControllerModelMbtIT} to replay traces generated from
 * a formal Quint specification (`UserOperatorModel.qnt`) and assert that real system behavior matches
 * the expected invariant-preserving execution.
 *
 * @see KafkaUserModelActions.ModelEvent
 * @see KafkaUserModelActions.EventsFactory
 * @see io.strimzi.operator.user.UserControllerModelMbtIT
 */
public class KafkaUserModelActions {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUserModelActions.class);
    private static final long POLL_INTERVAL_MS = Duration.ofMillis(100).toMillis();
    private static final long POLL_TIMEOUT_MS = Duration.ofMillis(15_000).toMillis();
    private static final Random RNG = new Random();

    private final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    private final SecretOperator secretOperator;
    private final String namespace;

    public KafkaUserModelActions(
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps,
        SecretOperator secretOperator,
        String namespace
    ) {
        this.kafkaUserOps = kafkaUserOps;
        this.secretOperator = secretOperator;
        this.namespace = namespace;
    }

    public enum Action {
        CREATE_USER("createUser"),
        UPDATE_USER("updateUser"),
        DELETE_USER("deleteUser"),
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
                if (a.actionName.equalsIgnoreCase(s)) {
                    return a;
                }
            }
            throw new IllegalArgumentException("Unknown action: " + s);
        }
    }

    sealed interface ModelEvent permits CreateUserEvent, UpdateUserEvent, DeleteUserEvent {
        void apply(final KafkaUserModelActions actions) throws Exception;
    }

    record CreateUserEvent(
        String username,
        String authType,
        Boolean quotasEnabled,
        Boolean aclsEnabled,
        String resourceType,
        String patternType,
        String operation
    ) implements ModelEvent {
        public void apply(final KafkaUserModelActions actions) {
            actions.createKafkaUser(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation);
        }
    }

    record UpdateUserEvent(String username,
                           String authType,
                           Boolean quotasEnabled,
                           Boolean aclsEnabled,
                           String resourceType,
                           String patternType,
                           String operation) implements ModelEvent {
        public void apply(final KafkaUserModelActions actions) {
            actions.updateKafkaUser(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation);
        }
    }

    record DeleteUserEvent(String username, String authType) implements ModelEvent {
        public void apply(final KafkaUserModelActions actions) {
            actions.deleteKafkaUser(username, authType);
        }
    }

    public static class EventsFactory {
        public static ModelEvent create(final String username,
                                        final String authType,
                                        final Boolean quotasEnabled,
                                        final Boolean aclsEnabled,
                                        final String resourceType,
                                        final String patternType,
                                        final String operation) {
            return new CreateUserEvent(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation);
        }

        public static ModelEvent update(final String username,
                                        final String authType,
                                        final Boolean quotasEnabled,
                                        final Boolean aclsEnabled,
                                        final String resourceType,
                                        final String patternType,
                                        final String operation) {
            return new UpdateUserEvent(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation);
        }

        public static ModelEvent delete(final String username,
                                        final String authType) {
            return new DeleteUserEvent(username, authType);
        }
    }

    /**
     * Processes the next ModelEvent from the event queue.
     * Handles empty queue gracefully.
     */
    public void processNextEvent(final List<ModelEvent> eventQueue) throws Exception {
        if (eventQueue.isEmpty()) {
            LOGGER.warn("⚠️ Tried to processNextEvent, but the event queue is empty.");
            return;
        }

        ModelEvent next = eventQueue.remove(0);
        LOGGER.info("🌀 Processing next event from queue: {}", next);
        next.apply(this);
    }

    public void createKafkaUser(final String username,
                                final String authType,
                                final Boolean quotasEnabled,
                                final Boolean aclsEnabled,
                                final String resourceType,
                                final String patternType,
                                final String operation) {
       final KafkaUserBuilder builder = new KafkaUserBuilder()
            .withNewMetadata()
                .withLabels(Labels.forStrimziCluster(ResourceUtils.CLUSTER_NAME).toMap())
                .withName(username)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .endSpec();

        // Authentication
        if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType)) {
            builder
                .editOrNewSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                .endSpec();
        } else if (KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
            builder
                .editOrNewSpec()
                    .withAuthentication(new KafkaUserTlsClientAuthentication())
                .endSpec();
        }

        // Quotas
        if (Boolean.TRUE.equals(quotasEnabled)) {
            builder
                .editOrNewSpec()
                    .withQuotas(new KafkaUserQuotasBuilder()
                        .withConsumerByteRate(100)
                        .withProducerByteRate(200)
                        .withRequestPercentage(50)
                        .withControllerMutationRate(5.0)
                .build())
                .endSpec();
        }

        // Authorization
        if (Boolean.TRUE.equals(aclsEnabled)) {
            builder
                .editOrNewSpec()
                    .withAuthorization(ResourceUtils.createSimpleAuthorization(resourceType, patternType, operation))
                .endSpec();
        }

        try {
            kafkaUserOps.resource(namespace, builder.build()).create();

            ResourceUtils.waitUntilKafkaUserReady(username, namespace, POLL_INTERVAL_MS, POLL_TIMEOUT_MS, kafkaUserOps);
            if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType) || KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                waitUntilSecretCreated(username, namespace, POLL_TIMEOUT_MS);
            }
        } catch (Exception e) {
            if (!e.getMessage().contains("409")) {
                throw e; // Re-throw unexpected exceptions
            }
            // Log conflict (409), resource already exists
            LOGGER.info("User '{}' already exists (409), asserting state matches model.", username);
            assertThat("KafkaUser should exist but does not!", kafkaUserOps.get(namespace, username), notNullValue());
        }
    }

    public void updateKafkaUser(final String username,
                                final String authType,
                                final Boolean quotasEnabled,
                                final Boolean aclsEnabled,
                                final String resourceType,
                                final String patternType,
                                final String operation) {
        retryOnConflict(() -> {
            final KafkaUser existing = kafkaUserOps.get(namespace, username);
            if (existing == null) {
                LOGGER.info("KafkaUser '{}' does not exist; skipping update.", username);
                return true;
            }

            KafkaUserBuilder builder = new KafkaUserBuilder(existing);

            builder.editOrNewMetadata().addToLabels("new-label", "" + new Random().nextInt(Integer.MAX_VALUE)).endMetadata();

            // Authentication
            if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType)) {
                builder
                    .editOrNewSpec()
                        .withNewKafkaUserScramSha512ClientAuthentication()
                        .endKafkaUserScramSha512ClientAuthentication()
                    .endSpec();
            } else if (KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                builder
                    .editOrNewSpec()
                        .withNewKafkaUserTlsClientAuthentication()
                        .endKafkaUserTlsClientAuthentication()
                    .endSpec();
            }

            // Quotas
            if (Boolean.TRUE.equals(quotasEnabled)) {
                builder.
                    editOrNewSpec()
                        .withQuotas(new KafkaUserQuotasBuilder()
                            .withConsumerByteRate(RNG.nextInt(1000) + 100)
                            .withProducerByteRate(RNG.nextInt(1000) + 200)
                            .withRequestPercentage(RNG.nextInt(100))
                            .withControllerMutationRate(RNG.nextDouble() * 10)
                        .build())
                    .endSpec();
            }

            // Authorization
            if (Boolean.TRUE.equals(aclsEnabled)) {
                if (resourceType != null && patternType != null && operation != null) {
                    builder.editOrNewSpec().withAuthorization(
                        new KafkaUserAuthorizationSimpleBuilder(
                            ResourceUtils.createSimpleAuthorization(resourceType, patternType, operation))
                            .build())
                    .endSpec();
                } else {
                    builder.editOrNewSpec().withAuthorization(
                        new KafkaUserAuthorizationSimpleBuilder(
                            ResourceUtils.createSimpleAuthorization(Set.of(AclOperation.values())))
                            .build())
                    .endSpec();
                }
            } else {
                builder
                    .editOrNewSpec()
                        .withAuthorization(null)
                    .endSpec();
            }

            // Always clear .status on update => This forces the controller to re-evaluate and not trust old Ready status.
            builder.withStatus(null);

            kafkaUserOps.resource(namespace, builder.build()).update();
            ResourceUtils.waitUntilKafkaUserReady(username, namespace, POLL_INTERVAL_MS, POLL_TIMEOUT_MS, kafkaUserOps);
            if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType) ||
                KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                waitUntilSecretCreated(username, namespace, POLL_TIMEOUT_MS);
            }

            return true;
        });
    }

    public void deleteKafkaUser(final String username,
                                final String authType) {
        final KafkaUser existing = kafkaUserOps.get(namespace, username);
        if (existing != null) {
            kafkaUserOps.resource(namespace, username).delete();
            kafkaUserOps.resource(namespace, username)
                .waitUntilCondition(u -> u == null, POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            ResourceUtils.waitUntilUserAndSecretDeleted(username, namespace, authType, POLL_INTERVAL_MS, POLL_TIMEOUT_MS, kafkaUserOps, secretOperator);
        } else {
            LOGGER.info("KafkaUser '{}' already deleted (404).", username);
        }
    }

    private <T> T retryOnConflict(Supplier<T> action) {
        final int maxRetries = 5;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                return action.get();
            } catch (KubernetesClientException e) {
                if (e.getCode() == 409 && attempt < maxRetries - 1) {
                    LOGGER.warn("Conflict detected, retrying... (attempt {})", attempt + 1);
                    continue;
                }
                throw e;
            }
        }
        throw new RuntimeException("Max retries exceeded due to conflict");
    }

    private void waitUntilSecretCreated(final String username,
                                        final String namespace,
                                        final long timeoutMillis) {
        TestUtils.waitFor(
            "Secret for KafkaUser " + username + " to be created",
            Duration.ofMillis(POLL_INTERVAL_MS).toMillis(),
            Duration.ofMillis(timeoutMillis).toMillis(),
            () -> secretOperator.get(namespace, username) != null
        );
    }

    public void waitUntilAllSecretsHaveMatchingKafkaUsers(final String namespace) {
        final long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            final Set<String> expectedSecrets = kafkaUserOps.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .filter(user -> {
                    String type = ResourceUtils.getAuthType(user);
                    return !"none".equalsIgnoreCase(type);
                })
                .map(user -> user.getMetadata().getName())
                .collect(Collectors.toSet());

            final Set<String> actualSecrets = secretOperator.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .map(secret -> secret.getMetadata().getName())
                .collect(Collectors.toSet());

            // Every actual secret must correspond to a user with authType != 'none'
            final boolean allSecretsMatchUsers = actualSecrets.stream().allMatch(expectedSecrets::contains);

            if (allSecretsMatchUsers) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        throw new RuntimeException("Timeout waiting for Secrets without corresponding KafkaUsers (authType != 'none') to be deleted");
    }
}
