package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;

import java.sql.SQLOutput;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class InvariantChecker {

    public final static Labels kafkaUserLabels = Labels.forStrimziCluster("my-cluster");

    private final CrdOperator<?, KafkaUser, ?> kafkaUserOps;
    private final SecretOperator secretOperator;

    public InvariantChecker(CrdOperator<?, KafkaUser, ?> kafkaUserOps, SecretOperator secretOperator) {
        this.kafkaUserOps = kafkaUserOps;
        this.secretOperator = secretOperator;
    }

    public void assertControllerAlive(UserController controller) {
        assertTrue(controller.isAlive(), "❌ Controller is not alive!");
    }

    public void assertUserConsistency(String namespace, String username) {
        if (username == null || username.isBlank()) {
            System.out.println("⚠️ Skipping invariant check: username is null or blank");
            return;
        }

        KafkaUser ku = kafkaUserOps.get(namespace, username);

        if (ku != null && "Ready".equals(statusOf(ku))) {
            if (requiresSecret(ku)) {
                Secret secret = secretOperator.get(namespace, username);
                assertTrue(secret != null, "❌ KafkaUser '" + username + "' is Ready but Secret is missing");
            } else {
                System.out.println("ℹ️ KafkaUser " + username + "' is Ready and does not require a Secret (authentication is none or tls-external)");
            }
        }
    }

    private boolean requiresSecret(KafkaUser ku) {
        if (ku.getSpec() == null || ku.getSpec().getAuthentication() == null) {
            return false; // No authentication -> no Secret needed
        }

        String authType = ku.getSpec().getAuthentication().getType();
        return "tls".equalsIgnoreCase(authType) || "scram-sha-512".equalsIgnoreCase(authType);
    }

    private String statusOf(KafkaUser ku) {
        return ku.getStatus() != null && ku.getStatus().getConditions() != null
            ? ku.getStatus().getConditions().stream()
            .filter(c -> "Ready".equals(c.getType()))
            .findFirst()
            .map(c -> c.getStatus())
            .orElse(null)
            : null;
    }

    public void assertSecretsConsistency(String namespace) {
        Set<String> secretNames = secretOperator.list(namespace, kafkaUserLabels).stream()
            .map(secret -> secret.getMetadata().getName())
            .collect(Collectors.toSet());

        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            boolean ready = user.getStatus() != null && user.getStatus().getConditions().stream()
                .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));

            if (ready) {
                if (requiresSecret(user)) {
                    assertTrue(secretNames.contains(user.getMetadata().getName()),
                        "❌ Secret missing for Ready KafkaUser that requires a Secret: " + user.getMetadata().getName());
                } else {
                    System.out.println("ℹ️ Ready KafkaUser '" + user.getMetadata().getName() + "' does not require a Secret (authentication is none or tls-external)");
                }
            }
        });
    }

    public void assertNoSecretsForDeletedUsers(String namespace) {
        Set<String> secretNames = secretOperator.list(namespace, kafkaUserLabels).stream()
            .map(secret -> secret.getMetadata().getName())
            .collect(Collectors.toSet());

        Set<String> existingKafkaUsers = kafkaUserOps.list(namespace, kafkaUserLabels).stream()
            .map(user -> user.getMetadata().getName())
            .collect(Collectors.toSet());

        for (String secretName : secretNames) {
            assertTrue(existingKafkaUsers.contains(secretName),
                "❌ Secret '" + secretName + "' exists without corresponding KafkaUser");
        }
    }

    private void waitUntilSecretExists(String namespace, String username, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (secretOperator.get(namespace, username) != null) {
                return;
            }
            Thread.sleep(100); // poll interval
        }
    }

    public void assertQuotasNonNegative(String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (user.getSpec() != null && user.getSpec().getQuotas() != null) {
                var quotas = user.getSpec().getQuotas();
                assertTrue(quotas.getProducerByteRate() >= 0,
                    "❌ producerByteRate is negative for user: " + user.getMetadata().getName());
                assertTrue(quotas.getConsumerByteRate() >= 0,
                    "❌ consumerByteRate is negative for user: " + user.getMetadata().getName());
                assertTrue(quotas.getRequestPercentage() >= 0,
                    "❌ requestPercentage is negative for user: " + user.getMetadata().getName());
                assertTrue(quotas.getControllerMutationRate() >= 0,
                    "❌ controllerMutationRate is negative for user: " + user.getMetadata().getName());
            }
        });
    }

    public void assertQuotasRequestPercentageValid(String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (user.getSpec() != null && user.getSpec().getQuotas() != null) {
                var quotas = user.getSpec().getQuotas();
                assertTrue(quotas.getRequestPercentage() >= 0 && quotas.getRequestPercentage() <= 100,
                    "❌ requestPercentage out of range [0, 100] for user: " + user.getMetadata().getName());
            }
        });
    }

    public void assertReadyUsersQuotasValid(String namespace) {
        kafkaUserOps.list(namespace, kafkaUserLabels).forEach(user -> {
            if (user.getStatus() != null
                && user.getStatus().getConditions() != null
                && user.getStatus().getConditions().stream()
                .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()))) {

                if (user.getSpec() != null && user.getSpec().getQuotas() != null) {
                    var quotas = user.getSpec().getQuotas();

                    assertTrue(quotas.getProducerByteRate() >= 0,
                        "❌ Ready user '" + user.getMetadata().getName() + "' has negative producerByteRate");
                    assertTrue(quotas.getConsumerByteRate() >= 0,
                        "❌ Ready user '" + user.getMetadata().getName() + "' has negative consumerByteRate");
                    assertTrue(quotas.getRequestPercentage() >= 0,
                        "❌ Ready user '" + user.getMetadata().getName() + "' has negative requestPercentage");
                    assertTrue(quotas.getRequestPercentage() <= 100,
                        "❌ Ready user '" + user.getMetadata().getName() + "' has requestPercentage > 100");
                    assertTrue(quotas.getControllerMutationRate() >= 0,
                        "❌ Ready user '" + user.getMetadata().getName() + "' has negative controllerMutationRate");
                }
                // else: user is Ready but has no quotas → this is allowed
            }
        });
    }

}