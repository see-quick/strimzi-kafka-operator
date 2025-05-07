/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.api.kafka.model.user.acl.AclRuleBuilder;
import io.strimzi.api.kafka.model.user.acl.AclRuleClusterResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleGroupResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleTopicResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleTransactionalIdResource;
import io.strimzi.api.kafka.model.user.acl.AclRuleType;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.UserOperatorConfig.UserOperatorConfigBuilder;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ResourceUtils {
    public static final Map<String, String> LABELS = Collections.singletonMap("foo", "bar");
    public static final String NAMESPACE = "namespace";
    public static final String NAME = "user";
    public static final String CA_CERT_NAME = "ca-cert";
    public static final String CA_KEY_NAME = "ca-key";
    public static final String PASSWORD = "my-password";
    public static final String CLUSTER_NAME = "my-cluster";

    public static UserOperatorConfig createUserOperatorConfig(String namespace, Map<String, String> labels, boolean aclsAdminApiSupported, String scramShaPasswordLength, String secretPrefix) {
        Map<String, String> envVars = new HashMap<>(4);
        envVars.put(UserOperatorConfig.NAMESPACE.key(), namespace);
        envVars.put(UserOperatorConfig.LABELS.key(), labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")));
        envVars.put(UserOperatorConfig.CA_CERT_SECRET_NAME.key(), CA_CERT_NAME);
        envVars.put(UserOperatorConfig.CA_KEY_SECRET_NAME.key(), CA_KEY_NAME);
        envVars.put(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.key(), Boolean.toString(aclsAdminApiSupported));

        if (!scramShaPasswordLength.equals("32")) {
            envVars.put(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), scramShaPasswordLength);
        }

        if (secretPrefix != null) {
            envVars.put(UserOperatorConfig.SECRET_PREFIX.key(), secretPrefix);
        }

        return UserOperatorConfig.buildFromMap(envVars);
    }

    public static UserOperatorConfig createUserOperatorConfigForUserControllerTesting(String namespace, Map<String, String> labels, int fullReconciliationInterval, int queueSize, int poolSize, String secretPrefix) {
        return new UserOperatorConfigBuilder(createUserOperatorConfig(namespace, labels, false, "32", secretPrefix))
                      .with(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key(), String.valueOf(fullReconciliationInterval))
                      .with(UserOperatorConfig.WORK_QUEUE_SIZE.key(), String.valueOf(queueSize))
                      .with(UserOperatorConfig.USER_OPERATIONS_THREAD_POOL_SIZE.key(), String.valueOf(poolSize))
                      .build();
    }

    public static UserOperatorConfig createUserOperatorConfigForUserControllerTesting(String namespace,
                                                                                      Map<String, String> labels,
                                                                                      int fullReconciliationInterval,
                                                                                      int queueSize,
                                                                                      int poolSize,
                                                                                      String secretPrefix,
                                                                                      int threadControllerSize,
                                                                                      boolean aclsAdminApiSupported) {
        return new UserOperatorConfigBuilder(createUserOperatorConfig(namespace, labels, aclsAdminApiSupported, "32", secretPrefix))
            .with(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.key(), String.valueOf(fullReconciliationInterval))
            .with(UserOperatorConfig.WORK_QUEUE_SIZE.key(), String.valueOf(queueSize))
            .with(UserOperatorConfig.USER_OPERATIONS_THREAD_POOL_SIZE.key(), String.valueOf(poolSize))
            .with(UserOperatorConfig.CONTROLLER_THREAD_POOL_SIZE.key(), String.valueOf(threadControllerSize))
            .build();
    }

    public static UserOperatorConfig createUserOperatorConfig(String namespace) {
        return createUserOperatorConfig(namespace, Map.of(), true, "32", null);
    }

    public static UserOperatorConfig createUserOperatorConfig() {
        return createUserOperatorConfig(NAMESPACE, Map.of(), true, "32", null);
    }

    public static UserOperatorConfig createUserOperatorConfig(String namespace, String scramShaPasswordLength) {
        return new UserOperatorConfigBuilder(createUserOperatorConfig(namespace))
                       .with(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.key(), scramShaPasswordLength)
                       .build();
    }

    public static KafkaUser createKafkaUser(String namespace, KafkaUserAuthentication authentication) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(NAME)
                    .withLabels(LABELS)
                .endMetadata()
                .withNewSpec()
                    .withAuthentication(authentication)
                    .withNewKafkaUserAuthorizationSimple()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic11").endAclRuleTopicResource()
                            .withOperations(AclOperation.READ, AclOperation.CREATE, AclOperation.WRITE)
                        .endAcl()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic")
                            .endAclRuleTopicResource()
                            .withOperations(AclOperation.DESCRIBE, AclOperation.READ)
                        .endAcl()
                    .endKafkaUserAuthorizationSimple()
                    .withNewQuotas()
                        .withConsumerByteRate(1_024 * 1_024)
                        .withProducerByteRate(1_024 * 1_024)
                    .endQuotas()
                .endSpec()
                .build();
    }

    public static KafkaUser createKafkaUser(String namespace, KafkaUserQuotas quotas) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(NAME)
                    .withLabels(LABELS)
                .endMetadata()
                .withNewSpec()
                    .withQuotas(quotas)
                    .withNewKafkaUserAuthorizationSimple()
                        .addNewAcl()
                            .withNewAclRuleTopicResource()
                                .withName("my-topic")
                            .endAclRuleTopicResource()
                        .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
                        .endAcl()
                        .addNewAcl()
                            .withNewAclRuleGroupResource()
                                .withName("my-group")
                            .endAclRuleGroupResource()
                            .withOperations(AclOperation.READ)
                        .endAcl()
                    .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();
    }

    public static KafkaUser createKafkaUserTls(String namespace) {
        return createKafkaUser(namespace, new KafkaUserTlsClientAuthentication());
    }

    public static KafkaUser createKafkaUserScramSha(String namespace) {
        return createKafkaUser(namespace, new KafkaUserScramSha512ClientAuthentication());
    }

    public static KafkaUser createKafkaUserQuotas(String namespace, Integer consumerByteRate, Integer producerByteRate, Integer requestPercentage, Double controllerMutationRate) {
        KafkaUserQuotas kuq = new KafkaUserQuotasBuilder()
                .withConsumerByteRate(consumerByteRate)
                .withProducerByteRate(producerByteRate)
                .withRequestPercentage(requestPercentage)
                .withControllerMutationRate(controllerMutationRate)
                .build();

        return createKafkaUser(namespace, kuq);
    }

    public static Secret createClientsCaCertSecret(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_CERT_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();
    }

    public static Secret createClientsCaKeySecret(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.CA_KEY_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .build();
    }

    public static Secret createUserSecretTls(String namespace, String name)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Labels.fromMap(LABELS)
                        .withKubernetesName(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withKubernetesInstance(name)
                        .withKubernetesPartOf(name)
                        .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                        .withStrimziKind(KafkaUser.RESOURCE_KIND)
                        .toMap())
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .addToData("user.key", Base64.getEncoder().encodeToString("expected-key".getBytes()))
                .addToData("user.crt", Base64.getEncoder().encodeToString("expected-crt".getBytes()))
                .addToData("user.p12", Base64.getEncoder().encodeToString("expected-p12".getBytes()))
                .addToData("user.password", Base64.getEncoder().encodeToString("expected-password".getBytes()))
                .build();
    }

    public static Secret createUserSecretTls(String namespace)  {
        return createUserSecretTls(namespace, NAME);
    }

    public static Secret createUserSecretScramSha(String namespace)  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(namespace)
                    .withLabels(Labels.fromMap(LABELS).withStrimziKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(PASSWORD.getBytes()))
                .addToData(KafkaUserModel.KEY_SASL_JAAS_CONFIG, Base64.getEncoder().encodeToString(KafkaUserModel.getSaslJsonConfig(NAME, PASSWORD).getBytes()))
                .build();
    }

    public static Set<SimpleAclRule> createExpectedSimpleAclRules(KafkaUser user) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<>();

        if (user.getSpec().getAuthorization() != null && KafkaUserAuthorizationSimple.TYPE_SIMPLE.equals(user.getSpec().getAuthorization().getType())) {
            KafkaUserAuthorizationSimple adapted = (KafkaUserAuthorizationSimple) user.getSpec().getAuthorization();

            if (adapted.getAcls() != null) {
                for (AclRule rule : adapted.getAcls()) {
                    simpleAclRules.addAll(SimpleAclRule.fromCrd(rule));
                }
            }
        }

        return simpleAclRules;
    }

    public static KafkaUserAuthorizationSimple createSimpleAuthorization(
        String resourceType,
        String patternType,
        String operation
    ) {
        final AclRuleResource resource;

        switch (resourceType.toLowerCase()) {
            case "topic" -> {
                AclRuleTopicResource r = new AclRuleTopicResource();
                r.setName("*");
                r.setAdditionalProperty("patternType", patternType);
                resource = r;
            }
            case "group" -> {
                AclRuleGroupResource r = new AclRuleGroupResource();
                r.setName("*");
                r.setAdditionalProperty("patternType", patternType);
                resource = r;
            }
            case "cluster" -> {
                AclRuleClusterResource r = new AclRuleClusterResource();
                r.setAdditionalProperty("patternType", patternType);
                resource = r;
            }
            case "transactionalid" -> {
                AclRuleTransactionalIdResource r = new AclRuleTransactionalIdResource();
                r.setName("*");
                r.setAdditionalProperty("patternType", patternType);
                resource = r;
            }
            default -> throw new IllegalArgumentException("Unsupported resourceType: " + resourceType);
        }

        final AclRule acl = new AclRuleBuilder()
            .withResource(resource)
            .withHost("*")
            .withOperation(AclOperation.valueOf(operation.toUpperCase()))
            .withType(AclRuleType.ALLOW)
            .build();


        return new KafkaUserAuthorizationSimpleBuilder()
            .withAcls(List.of(acl))
            .build();
    }

    public static KafkaUserAuthorizationSimple createSimpleAuthorization(Set<AclOperation> operations) {
        return new KafkaUserAuthorizationSimpleBuilder()
            .withAcls(List.of(
                new AclRuleBuilder()
                    .withNewAclRuleTopicResource()
                    .withName("*")
                    .endAclRuleTopicResource()
                    .withOperations(new ArrayList<>(operations))
                    .withHost("*")
                    .withType(AclRuleType.ALLOW)
                    .build()
            ))
            .build();
    }

    public static String getAuthType(KafkaUser user) {
        if (user.getSpec() == null || user.getSpec().getAuthentication() == null) {
            return "none";
        }
        return user.getSpec().getAuthentication().getType();
    }

    public static void waitUntilKafkaUserReady(final String username,
                                               final String namespace,
                                               final long pollingInterval,
                                               final long timeoutMillis,
                                               final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps) {
        TestUtils.waitFor(
            "KafkaUser " + username + " to become Ready",
            Duration.ofMillis(pollingInterval).toMillis(),
            Duration.ofMillis(timeoutMillis).toMillis(),
            () -> {
                KafkaUser ku = kafkaUserOps.get(namespace, username);
                return ku != null
                    && ku.getStatus() != null
                    && ku.getStatus().getConditions() != null
                    && ku.getStatus().getConditions().stream()
                    .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
            }
        );
    }

    public static void waitUntilKafkaReady(final String bootstrapServers,
                                           final Duration timeout) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            TestUtils.waitFor(
                "Kafka broker to be ready",
                500,
                timeout.toMillis(),
                () -> {
                    try {
                        return !admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).isEmpty();
                    } catch (Exception e) {
                        return false;
                    }
                }
            );
        }
    }

    public static void waitUntilUserAndSecretDeleted(final String username,
                                              final String namespace,
                                              final String authType,
                                              final long pollingInterval,
                                              final long timeoutMillis,
                                              final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps,
                                              final SecretOperator secretOperator) {
        TestUtils.waitFor(
            "KafkaUser " + username + " and Secret to be deleted",
            Duration.ofMillis(pollingInterval).toMillis(),
            Duration.ofMillis(timeoutMillis).toMillis(),
            () -> {
                boolean userDeleted = kafkaUserOps.get(namespace, username) == null;

                if (authType == null || Objects.equals(authType, "none")) {
                    return userDeleted;
                } else {
                    boolean secretDeleted = secretOperator.get(namespace, username) == null;
                    return userDeleted && secretDeleted;
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    public static Object unwrapOptional(Object raw) {
        if (raw instanceof Map<?, ?> wrapper && "Some".equals(wrapper.get("tag"))) {
            return wrapper.get("value");
        }
        return null;
    }

    public static <T> T getOptionalValue(Map<String, Object> nondet, String key, Class<T> expectedType) {
        Object unwrapped = unwrapOptional(nondet.get(key));
        if (expectedType.isInstance(unwrapped)) {
            return expectedType.cast(unwrapped);
        }
        return null;
    }

    public static List<String> getTupleEnumTags(final Map<String, Object> nondet,
                                                final String key,
                                                final int size) {
        final Object maybeSome = unwrapOptional(nondet.get(key));
        if (maybeSome instanceof Map<?, ?> maybeTuple) {
            final Object inner = maybeTuple.get("#tup");
            if (inner instanceof List<?> list && list.size() == size) {
                return list.stream()
                    .map(e -> ((Map<?, ?>) e).get("tag"))
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .collect(Collectors.toList());
            }
        }
        return List.of();
    }
}
