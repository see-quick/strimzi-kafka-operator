package io.strimzi.operator.user;

import com.google.common.base.Supplier;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

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

    public void createKafkaUser(String username, String authType, Boolean quotasEnabled, Boolean aclsEnabled) throws Exception {
        KafkaUserBuilder builder = new KafkaUserBuilder()
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
                    .withAuthorization(ResourceUtils.createSimpleAuthorization(Set.of(AclOperation.READ)))
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

    public void updateKafkaUser(String username, String authType, Boolean quotasEnabled, Boolean aclsEnabled) throws Exception {
        retryOnConflict(() -> {
            KafkaUser existing = kafkaUserOps.get(namespace, username);
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
                // TODO: another flag to use custom scram sha password instead of randon one )) and use this as code
                //  // when also Quotas we also update custom scram-sha
                //                final String secretName = "custom-secret-scram-sha";
                //                final Secret userDefinedSecret = new SecretBuilder()
                //                    .withNewMetadata()
                //                    .withName(secretName)
                //                    .withNamespace(namespace)
                //                    .endMetadata()
                //                    .addToData("password", "VDZmQ2pNMWRRb1d6VnBYNWJHa1VSOGVOMmFIeFA3WXM=")
                //                    .build();
                //                client.resource(userDefinedSecret).create();
                //                existing.getSpec().setAuthentication(
                //                    new KafkaUserScramSha512ClientAuthenticationBuilder()
                //                        .withPassword(new PasswordBuilder()
                //                            .editOrNewValueFrom()
                //                                .withNewSecretKeyRef("password", secretName, false)
                //                            .endValueFrom()
                //                            .build())
                //                        .build()
                //
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
                        // TODO: maybe make it also in (generate random attributes Quint model)?
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
                builder.editOrNewSpec().withAuthorization(
                    new KafkaUserAuthorizationSimpleBuilder(
                        // TODO: maybe all values in Quint model? => I think it's not needed :)
                        ResourceUtils.createSimpleAuthorization(Set.of(AclOperation.values())))
                    .build())
                .endSpec();
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
            if (KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(authType) || KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(authType)) {
                waitUntilSecretCreated(username, namespace, POLL_TIMEOUT_MS);
            }

            return true;
        });
    }

    public void deleteKafkaUser(String username, String authType) {
        KafkaUser existing = kafkaUserOps.get(namespace, username);
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

    private void waitUntilSecretCreated(String username, String namespace, long timeoutMillis) {
        TestUtils.waitFor(
            "Secret for KafkaUser " + username + " to be created",
            Duration.ofMillis(POLL_INTERVAL_MS).toMillis(),
            Duration.ofMillis(timeoutMillis).toMillis(),
            () -> secretOperator.get(namespace, username) != null
        );
    }

    public void waitUntilAllSecretsHaveMatchingKafkaUsers(String namespace) {
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            Set<String> expectedSecrets = kafkaUserOps.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .filter(user -> {
                    String type = ResourceUtils.getAuthType(user);
                    return !"none".equalsIgnoreCase(type);
                })
                .map(user -> user.getMetadata().getName())
                .collect(Collectors.toSet());

            Set<String> actualSecrets = secretOperator.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .map(secret -> secret.getMetadata().getName())
                .collect(Collectors.toSet());

            // Every actual secret must correspond to a user with authType != 'none'
            boolean allSecretsMatchUsers = actualSecrets.stream().allMatch(expectedSecrets::contains);

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
