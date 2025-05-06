package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserSpec;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime Invariant Checker for the Strimzi User Operator.
 *
 * This utility asserts the consistency of KafkaUser resources and associated artifacts:
 * - Secrets must exist for users with SCRAM/TLS and "Ready" status.
 * - Deleted users must not retain ACLs or Secrets.
 * - Quotas must remain valid and non-negative.
 * - ACL rules must be present and well-formed when enabled.
 *
 * These checks directly mirror the invariants defined in the formal Quint model
 * (`UserOperatorModel.qnt`) and act as oracles during MBT trace replay.
 */
public class InvariantChecker {
    private static final Logger LOGGER = LogManager.getLogger(InvariantChecker.class.getName());
    public final static Labels kafkaUserLabels = Labels.forStrimziCluster("my-cluster");

    private final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    private final SecretOperator secretOperator;

    public InvariantChecker(final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps,
                            final SecretOperator secretOperator) {
        this.kafkaUserOps = kafkaUserOps;
        this.secretOperator = secretOperator;
    }

    public void assertControllerAlive(final UserController controller) {
        assertTrue(controller.isAlive(), "❌ Controller is not alive!");
    }

    public void assertUserConsistency(final String namespace,
                                      final String username) {
        if (username == null || username.isBlank()) {
            LOGGER.warn("⚠️ Skipping invariant check: username is null or blank");
            return;
        }

        final KafkaUser kafkaUser = kafkaUserOps.get(namespace, username);
        if (kafkaUser != null && hasStatusCondition(kafkaUser, "Ready", "True")) {
            if (requiresSecret(kafkaUser)) {
                Secret secret = secretOperator.get(namespace, username);
                assertTrue(secret != null, "❌ KafkaUser '" + username + "' is Ready but Secret is missing");
            } else {
                LOGGER.info("ℹ️ KafkaUser '{}' is Ready and does not require a Secret", username);
            }
        }
    }

    public void assertSecretsConsistency(final String namespace) {
        final Set<String> secretNames = secretOperator.list(namespace, kafkaUserLabels).stream()
            .map(secret -> secret.getMetadata().getName())
            .collect(Collectors.toSet());

        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (hasStatusCondition(user, "Ready", "True")) {
                if (requiresSecret(user)) {
                    assertTrue(secretNames.contains(user.getMetadata().getName()),
                        "❌ Secret missing for Ready KafkaUser: " + user.getMetadata().getName());
                } else {
                    LOGGER.info("ℹ️ Ready KafkaUser '{}' does not require a Secret", user.getMetadata().getName());
                }
            }
        });
    }

    public void assertNoSecretsForDeletedUsers(final String namespace) {
        final Set<String> secretNames = secretOperator.list(namespace, kafkaUserLabels).stream()
            .map(secret -> secret.getMetadata().getName())
            .collect(Collectors.toSet());

        final Set<String> kafkaUserNames = kafkaUserOps.list(namespace, kafkaUserLabels).stream()
            .map(user -> user.getMetadata().getName())
            .collect(Collectors.toSet());

        for (final String secretName : secretNames) {
            assertTrue(kafkaUserNames.contains(secretName),
                "❌ Secret '" + secretName + "' exists without corresponding KafkaUser");
        }
    }

    public void assertQuotasNonNegative(final String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> Optional.ofNullable(user.getSpec()).map(KafkaUserSpec::getQuotas).ifPresent(quotas -> {
            assertTrue(quotas.getProducerByteRate() >= 0, "❌ Negative producerByteRate for user: " + user.getMetadata().getName());
            assertTrue(quotas.getConsumerByteRate() >= 0, "❌ Negative consumerByteRate for user: " + user.getMetadata().getName());
            assertTrue(quotas.getRequestPercentage() >= 0, "❌ Negative requestPercentage for user: " + user.getMetadata().getName());
            assertTrue(quotas.getControllerMutationRate() >= 0, "❌ Negative controllerMutationRate for user: " + user.getMetadata().getName());
        }));
    }

    public void assertQuotasRequestPercentageValid(String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> Optional.ofNullable(user.getSpec()).map(KafkaUserSpec::getQuotas).ifPresent(quotas -> {
            assertTrue(quotas.getRequestPercentage() >= 0 && quotas.getRequestPercentage() <= 100,
                "❌ requestPercentage out of range [0,100] for user: " + user.getMetadata().getName());
        }));
    }

    public void assertReadyUsersQuotasValid(final String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (hasStatusCondition(user, "Ready", "True")) {
                Optional.ofNullable(user.getSpec()).map(KafkaUserSpec::getQuotas).ifPresent(quotas -> {
                    assertTrue(quotas.getProducerByteRate() >= 0, "❌ Negative producerByteRate for Ready user: " + user.getMetadata().getName());
                    assertTrue(quotas.getConsumerByteRate() >= 0, "❌ Negative consumerByteRate for Ready user: " + user.getMetadata().getName());
                    assertTrue(quotas.getRequestPercentage() >= 0, "❌ Negative requestPercentage for Ready user: " + user.getMetadata().getName());
                    assertTrue(quotas.getRequestPercentage() <= 100, "❌ requestPercentage > 100 for Ready user: " + user.getMetadata().getName());
                    assertTrue(quotas.getControllerMutationRate() >= 0, "❌ Negative controllerMutationRate for Ready user: " + user.getMetadata().getName());
                });
            }
        });
    }

    public void assertACLsExistForAuthorizedUsers(final String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (user.getSpec() != null && user.getSpec().getAuthorization() instanceof KafkaUserAuthorizationSimple simpleAuth) {
                List<AclRule> acls = simpleAuth.getAcls();
                if (acls != null) {
                    acls.forEach(rule -> {
                        assertTrue(rule.getResource() != null, "❌ ACL resource is missing for user: " + user.getMetadata().getName());
                        assertTrue(rule.getResource().getType() != null && !rule.getResource().getType().isEmpty(), "❌ ACL type is missing for user: " + user.getMetadata().getName());
                        assertTrue(rule.getHost() != null && !rule.getHost().isEmpty(), "❌ ACL host is missing for user: " + user.getMetadata().getName());
                    });
                }
            }
        });
    }

    public void assertReadyUsersMustHaveACLs(final String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (hasStatusCondition(user, "Ready", "True") &&
                user.getSpec() != null &&
                user.getSpec().getAuthorization() instanceof KafkaUserAuthorizationSimple simpleAuth) {

                List<AclRule> acls = simpleAuth.getAcls();
                assertTrue(acls != null && !acls.isEmpty(),
                    "❌ Ready user '" + user.getMetadata().getName() + "' must have at least one ACL rule");
            }
        });
    }

    private boolean hasStatusCondition(final KafkaUser user,
                                       final String type,
                                       final String status) {
        return Optional.ofNullable(user.getStatus())
            .map(s -> s.getConditions())
            .map(conds -> conds.stream().anyMatch(c -> type.equals(c.getType()) && status.equals(c.getStatus())))
            .orElse(false);
    }

    private boolean requiresSecret(final KafkaUser user) {
        return Optional.ofNullable(user.getSpec())
            .map(KafkaUserSpec::getAuthentication)
            .map(auth -> {
                        String type = auth.getType();
                        return KafkaUserTlsClientAuthentication.TYPE_TLS.equalsIgnoreCase(type) || KafkaUserScramSha512ClientAuthentication.TYPE_SCRAM_SHA_512.equalsIgnoreCase(type);
                }).orElse(false);
    }
}