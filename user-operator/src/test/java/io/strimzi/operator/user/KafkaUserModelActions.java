package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.SecretBuilder;
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
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    private final SecretOperator secretOperator;
    private final String namespace;
    private final List<String> processedStepsLog;

    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;

    public KafkaUserModelActions(
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps,
        SecretOperator secretOperator,
        String namespace,
        List<String> processedStepsLog
    ) {
        this.kafkaUserOps = kafkaUserOps;
        this.secretOperator = secretOperator;
        this.namespace = namespace;
        this.processedStepsLog = processedStepsLog;
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
        String operation,
        Boolean reconciliationPaused,
        Boolean useDesiredPassword
    ) implements ModelEvent {
        public void apply(final KafkaUserModelActions actions) {
            actions.createKafkaUser(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation, reconciliationPaused, useDesiredPassword);
        }
    }

    record UpdateUserEvent(String username,
                           String authType,
                           Boolean quotasEnabled,
                           Boolean aclsEnabled,
                           String resourceType,
                           String patternType,
                           String operation,
                           Boolean reconciliationPaused,
                           Boolean useDesiredPassword) implements ModelEvent {
        public void apply(final KafkaUserModelActions actions) {
            actions.updateKafkaUser(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation, reconciliationPaused, useDesiredPassword);
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
                                        final String operation,
                                        final Boolean reconciliationPaused,
                                        final Boolean useDesiredPassword) {
            return new CreateUserEvent(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation,  reconciliationPaused, useDesiredPassword);
        }

        public static ModelEvent update(final String username,
                                        final String authType,
                                        final Boolean quotasEnabled,
                                        final Boolean aclsEnabled,
                                        final String resourceType,
                                        final String patternType,
                                        final String operation,
                                        final Boolean reconciliationPaused,
                                        final Boolean useDesiredPassword) {
            return new UpdateUserEvent(username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation, reconciliationPaused, useDesiredPassword);
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

        // Log only executed actions
        processedStepsLog.add("🌀 Processed: " + next.toString());

        next.apply(this);
    }

    public void createKafkaUser(final String username,
                                final String authType,
                                final Boolean quotasEnabled,
                                final Boolean aclsEnabled,
                                final String resourceType,
                                final String patternType,
                                final String operation,
                                final Boolean reconciliationPaused,
                                final Boolean useDesiredPassword) {
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
            if (useDesiredPassword != null && useDesiredPassword) {
                builder
                    .editOrNewSpec()
                        .withNewKafkaUserScramSha512ClientAuthentication()
                            .withNewPassword()
                                .withNewValueFrom()
                                    .withNewSecretKeyRef("my-password", username + "-custom", false)
                                .endValueFrom()
                            .endPassword()
                    .endKafkaUserScramSha512ClientAuthentication()
                    .endSpec();

                if (Boolean.FALSE.equals(reconciliationPaused)) {
                    secretOperator.resource(namespace, new SecretBuilder()
                            .withNewMetadata()
                            .withName(username + "-custom")
                            .withNamespace(namespace)
                            .endMetadata()
                            .withData(Map.of("my-password", Util.encodeToBase64("desiredpassword")))
                            .build())
                        .createOrReplace();
                }
            } else {
                builder
                    .editOrNewSpec()
                        .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                    .endSpec();
            }
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

        if (Boolean.TRUE.equals(reconciliationPaused)) {
            builder
                .editOrNewMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true")
                .endMetadata();
        }

        try {
            kafkaUserOps.resource(namespace, builder.build()).create();

            if (Boolean.FALSE.equals(reconciliationPaused)) {
                ResourceUtils.waitUntilKafkaUserReady(username, namespace, POLL_INTERVAL_MS, POLL_TIMEOUT_MS, kafkaUserOps);
            }

            if ((KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType) &&
                (useDesiredPassword == null || !useDesiredPassword)) ||
                KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                waitUntilSecretCreated(username, namespace, POLL_TIMEOUT_MS);
            }
        } catch (KubernetesClientException e) {
            int code = e.getCode();

            if (code == 409) {
                LOGGER.info("User '{}' already exists (409), asserting state matches model.", username);
                assertThat("KafkaUser should exist but does not!", kafkaUserOps.get(namespace, username), notNullValue());
            } else if (code == 404) {
                LOGGER.warn("KafkaUser '{}' not found (404) when attempting creation. Skipping or retrying may be necessary.", username);
                // Depending on logic, you could choose to retry, ignore, or escalate
                throw e;
            } else if (code >= 500 && code < 600) {
                LOGGER.error("Server error ({}): {} while creating KafkaUser '{}'", code, e.getMessage(), username);
                // Depending on test policy, could retry or throw
                throw e;
            } else {
                throw e; // Propagate other exceptions
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public void updateKafkaUser(final String username,
                                final String authType,
                                final Boolean quotasEnabled,
                                final Boolean aclsEnabled,
                                final String resourceType,
                                final String patternType,
                                final String operation,
                                final Boolean reconciliationPaused,
                                final Boolean useDesiredPassword) {
        try {
            final KafkaUser existing = kafkaUserOps.get(namespace, username);
            if (existing == null) {
                LOGGER.info("KafkaUser '{}' does not exist; skipping update.", username);
                return;
            }

            KafkaUserBuilder builder = new KafkaUserBuilder(existing);

            builder.editOrNewMetadata().addToLabels("new-label", "" + new Random().nextInt(Integer.MAX_VALUE)).endMetadata();

            // Authentication
            if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType)) {
                if (useDesiredPassword != null && useDesiredPassword) {
                    builder
                        .editOrNewSpec()
                            .withNewKafkaUserScramSha512ClientAuthentication()
                                .withNewPassword()
                                    .withNewValueFrom()
                                        .withNewSecretKeyRef("my-password", username + "-custom", false)
                                    .endValueFrom()
                                .endPassword()
                            .endKafkaUserScramSha512ClientAuthentication()
                        .endSpec();

                    if (Boolean.FALSE.equals(reconciliationPaused)) {
                        secretOperator.resource(namespace, new SecretBuilder()
                                .withNewMetadata()
                                .withName(username + "-custom")
                                .withNamespace(namespace)
                                .endMetadata()
                                .withData(Map.of("my-password", Util.encodeToBase64("desiredpassword")))
                                .build())
                            .createOrReplace();
                    }
                } else {
                    builder
                        .editOrNewSpec()
                            .withNewKafkaUserScramSha512ClientAuthentication()
                            .endKafkaUserScramSha512ClientAuthentication()
                        .endSpec();
                }

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
                            .withRequestPercentage(1 + RNG.nextInt(100)) // Quota request_percentage must be greater than 0
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

            if (Boolean.TRUE.equals(reconciliationPaused)) {
                builder
                    .editOrNewMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true")
                    .endMetadata();
            }

            // Always clear .status on update => This forces the controller to re-evaluate and not trust old Ready status.
            builder.withStatus(null);

            kafkaUserOps.resource(namespace, builder.build()).update();

            if (Boolean.FALSE.equals(reconciliationPaused)) {
                ResourceUtils.waitUntilKafkaUserReady(username, namespace, POLL_INTERVAL_MS, POLL_TIMEOUT_MS, kafkaUserOps);
            }

            if ((KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType) &&
                (useDesiredPassword == null || !useDesiredPassword)) ||
                KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                waitUntilSecretCreated(username, namespace, POLL_TIMEOUT_MS);
            }
        } catch (KubernetesClientException e) {
            int code = e.getCode();
            if (code == 404) {
                LOGGER.warn("KafkaUser '{}' not found (404) during update; skipping.", username);
            } else if (code == 409) {
                LOGGER.info("Conflict (409) while updating '{}'; assuming state is consistent.", username);
            } else if (code >= 500 && code < 600) {
                LOGGER.error("Server error ({}): {} during update of KafkaUser '{}'", code, e.getMessage(), username);
                throw e;
            } else {
                throw e;
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error while updating KafkaUser '{}': {}", username, e.getMessage());
            throw new RuntimeException(e);
        }
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

    private void waitUntilSecretCreated(final String username,
                                        final String namespace,
                                        final long timeoutMillis) {
        KafkaUser ku = kafkaUserOps.get(namespace, username);
        boolean paused = ku != null &&
            ku.getMetadata() != null &&
            ku.getMetadata().getAnnotations() != null &&
            Boolean.TRUE.toString().equalsIgnoreCase(ku.getMetadata().getAnnotations().get(Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION));

        if (paused) {
            LOGGER.info("⏸️ Reconciliation is paused for '{}'; skipping secret wait", username);
            return;
        }

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
