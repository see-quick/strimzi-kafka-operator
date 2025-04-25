package io.strimzi.operator.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.operator.DisabledSimpleAclOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.QuotasOperator;
import io.strimzi.operator.user.operator.ScramCredentialsOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

public class UserControllerModelMbtIT {
    private static final Logger LOGGER = LogManager.getLogger(UserControllerModelMbtIT.class);
    private static final int POLL_INTERVAL_MS = 100;

    private static KubernetesClient client;
    private static MockKube3 mockKube;
    private static StrimziKafkaCluster kafkaCluster;
    private static Admin adminClient;

    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    // operators
    private SecretOperator secretOperator;
    private ScramCredentialsOperator scramCredentialsOperator;
    private QuotasOperator quotasOperator;
    private KafkaUserOperator kafkaUserOperator;

    private String namespace;
    private CertManager certManager;
    private UserController controller;
    private UserOperatorConfig config;

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Trace {
        public List<Map<String, Object>> states;
    }

    static Stream<String> traceProvider() {
        try {
            File dir = new File(UserControllerMockTest.class.getResource("/specification/traces/").toURI());
            return Stream.of(dir.listFiles())
                .filter(f -> f.getName().endsWith(".json"))
                .map(f -> "/specification/traces/" + f.getName());
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    @ParameterizedTest
    @MethodSource("traceProvider")
    public void testUserOperator(String tracePath) throws Exception {
        final List<String> mbtTimeline = new ArrayList<>();

        final ObjectMapper mapper = new ObjectMapper();
        final InputStream input = getClass().getResourceAsStream(tracePath);
        final Trace trace = mapper.readValue(input, Trace.class);

        // assumption is that trace would have always more than 0 states
        final Map<String, Object> parameters = (Map<String, Object>) trace.states.get(0).get("parameters");

        final Boolean aclsEnabled = (Boolean) parameters.get("aclsEnabled");
        final List<String> usersToTest = (List<String>) ((Map<String, Object>) parameters.get("potentialUsers")).get("#set");

        LOGGER.info("\n\n====================");
        LOGGER.info("📋 STARTING TEST CASE: {}", tracePath);
        LOGGER.info("====================");
        LOGGER.info("1️⃣ PARAMETER aclEnabled = {}", aclsEnabled);
        LOGGER.info("2️⃣ PARAMETER potentialUsers = {}", usersToTest);
        LOGGER.info("====================\n\n");

        config = ResourceUtils.createUserOperatorConfigForUserControllerTesting(
            namespace,
            Map.of(),
            1000,
            100,          // Batch queue size
            1,                     // Max batch size
            "",                     // Optional secret prefix
            1,                      // controller thread size
            aclsEnabled             // ACLs enabled/disabled
        );

        scramCredentialsOperator = new ScramCredentialsOperator(adminClient, config, ForkJoinPool.commonPool());
        quotasOperator = new QuotasOperator(adminClient, config, ForkJoinPool.commonPool());
        certManager = new MockCertManager();

        // Prepare controller
        final MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());

        kafkaUserOperator = new KafkaUserOperator(
            config,
            certManager,
            secretOperator,
            kafkaUserOps,
            scramCredentialsOperator,
            quotasOperator,
            config.isAclsAdminApiSupported() ?
                new SimpleAclOperator(adminClient, config, ForkJoinPool.commonPool()) :
                new DisabledSimpleAclOperator()
        );

        controller = new UserController(
            config,
            secretOperator,
            kafkaUserOps,
            kafkaUserOperator,
            metrics
        );

        kafkaUserOperator.start();
        controller.start();

        try {
            for (int i = 0; i < trace.states.size(); i++) {
                Map<String, Object> state = trace.states.get(i);
                String action = (String) state.get("mbt::actionTaken");

                // TODO: pick also numbers from model (i.e., quotas parametsrs stutff from Quint...)
                String username = null;
                String authType = null;
                Boolean quotasEnabled = null;

                Map<String, Object> nondet = (Map<String, Object>) state.get("mbt::nondetPicks");
                if (nondet != null) {
                    if (nondet.get("u") instanceof Map<?,?> uMap && "Some".equals(uMap.get("tag"))) {
                        username = ((String) uMap.get("value")).toLowerCase(Locale.ROOT);
                    }
                    if (nondet.get("authType") instanceof Map<?,?> authMap && "Some".equals(authMap.get("tag"))) {
                        authType = (String) authMap.get("value");
                    }
                    if (nondet.get("quotasEnabled") instanceof Map<?,?> quotasMap && "Some".equals(quotasMap.get("tag"))) {
                        quotasEnabled = (Boolean) quotasMap.get("value");
                    }
                }

                InvariantChecker invariants = new InvariantChecker(kafkaUserOps, secretOperator);

                String stepInfo = String.format(
                    "MBT: [%d] Executing action '%s' for user='%s', authType='%s', quotasEnabled='%s', aclsEnabled='%s'",
                    i, action, username, authType, quotasEnabled, aclsEnabled
                );
                LOGGER.info(stepInfo);
                mbtTimeline.add(stepInfo);

                if (i > 0) {
                    switch (action) {
                        case "createUser" -> createKafkaUser(username, authType, quotasEnabled, aclsEnabled);
                        case "updateUser" -> updateKafkaUser(username, authType, quotasEnabled, aclsEnabled);
                        case "deleteUser" -> {
                            KafkaUser existing = kafkaUserOps.get(namespace, username);
                            if (existing != null) {
                                kafkaUserOps.resource(namespace, username).delete();
                                kafkaUserOps.resource(namespace, username)
                                    .waitUntilCondition(u -> u == null, 10_000, TimeUnit.MILLISECONDS);

                                waitUntilUserAndSecretDeleted(username, namespace, authType, 15_000);
                            } else {
                                LOGGER.info("KafkaUser '{}' already deleted (404).", username);
                            }
                        }
                        case "" -> { /* no-op */ }
                        default -> { /* no-op */ }
                    }
                }

                waitUntilNoOrphanSecrets(namespace, 15_000);

                invariants.assertControllerAlive(controller);
                invariants.assertUserConsistency(namespace, username);
                // Secret invariants
                invariants.assertSecretsConsistency(namespace);
                invariants.assertNoSecretsForDeletedUsers(namespace);
                // Quotas invariants
                invariants.assertQuotasNonNegative(namespace);
                invariants.assertQuotasRequestPercentageValid(namespace);
                invariants.assertReadyUsersQuotasValid(namespace);
                // ACLs invariants
                invariants.assertACLsExistForAuthorizedUsers(namespace);
                invariants.assertNoACLsForDeletedUsers(namespace);
                invariants.assertReadyUsersMustHaveACLs(namespace);
            }
        } finally {
            kafkaUserOperator.stop();
            controller.stop();
            LOGGER.info("ℹ️ MBT test trace for {}. Timeline of actions:", tracePath);
            mbtTimeline.forEach(step -> LOGGER.info(step));
        }
    }

    @BeforeAll
    public static void beforeAll() {
        Map<String, String> additionalConfiguration = Map.of(
            "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "super.users", "User:ANONYMOUS");

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork()
            .withAdditionalKafkaConfiguration(additionalConfiguration)
            .build();
        kafkaCluster.start();

        waitUntilKafkaReady(kafkaCluster.getBootstrapServers(), Duration.ofSeconds(10));

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        adminClient = AdminClient.create(props);
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaUserCrd()
            .withDeletionController()
            .build();
        mockKube.start();
        client = mockKube.client();

        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);

        // Wait until the namespace is truly gone
        mockKube.prepareNamespace(namespace);

        secretOperator = new SecretOperator(ForkJoinPool.commonPool(), client);

        // ✅ Create dummy ca-cert Secret required by KafkaUserOperator
        Secret caCert = new SecretBuilder()
            .withNewMetadata()
                .withName("ca-cert")
                .withNamespace(namespace)
                    .addToAnnotations("strimzi.io/ca-cert-generation", "1")
            .endMetadata()
            .withData(Map.of("ca.crt", "ZHVtbXk=")) // "dummy" base64-encoded
            .build();
        // Create dummy ca-key Secret required by KafkaUserOperator
        Secret caKey = new SecretBuilder()
            .withNewMetadata()
                .withName("ca-key")
                .withNamespace(namespace)
                .addToAnnotations("strimzi.io/ca-key-generation", "1")
            .endMetadata()
            .withData(Map.of("ca.key", "dGVzdA==")) // "dummy" base64-encoded
            .build();

        secretOperator.resource(namespace, caCert).createOrReplace();
        secretOperator.resource(namespace, caKey).createOrReplace();

        kafkaUserOps = new CrdOperator<>(ForkJoinPool.commonPool(), client, KafkaUser.class, KafkaUserList.class, "KafkaUser");
    }

    @AfterEach
    public void afterEach() {
        if (mockKube != null) {
            mockKube.stop();
        }
    }

    @AfterAll
    public static void teardownKafka() {
        if (adminClient != null) adminClient.close();
        if (kafkaCluster != null) kafkaCluster.stop();
    }

    private void waitUntilNoOrphanSecrets(String namespace, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;

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

    private void waitUntilKafkaUserReady(String username, String namespace, long timeoutMillis) {
        TestUtils.waitFor(
            "KafkaUser " + username + " to become Ready",
            Duration.ofMillis(POLL_INTERVAL_MS).toMillis(),
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

    private void waitUntilUserAndSecretDeleted(String username, String namespace, String authType, long timeoutMillis) {
        TestUtils.waitFor(
            "KafkaUser " + username + " and Secret to be deleted",
            Duration.ofMillis(POLL_INTERVAL_MS).toMillis(),
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

    private static void waitUntilKafkaReady(String bootstrapServers, Duration timeout) {
        Properties props = new Properties();
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

    private void createKafkaUser(String username, String authType, Boolean quotasEnabled, Boolean aclsEnabled) throws Exception {
        KafkaUserBuilder builder = new KafkaUserBuilder()
            .withNewMetadata()
                .withLabels(Labels.forStrimziCluster("my-cluster").toMap())
                .withName(username)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .endSpec();

        // Authentication
        if ("scramsha".equalsIgnoreCase(authType)) {
            builder
                .editOrNewSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                .endSpec();
        } else if ("tls".equalsIgnoreCase(authType)) {
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

            waitUntilKafkaUserReady(username, namespace, 15_000);
        } catch (Exception e) {
            if (!e.getMessage().contains("409")) {
                throw e; // Re-throw unexpected exceptions
            }
            // Log conflict (409), resource already exists
            LOGGER.info("User '{}' already exists (409), asserting state matches model.", username);
            assertThat("KafkaUser should exist but does not!", kafkaUserOps.get(namespace, username), notNullValue());
        }
    }

    private void updateKafkaUser(String username, String authType, Boolean quotasEnabled, Boolean aclsEnabled) throws Exception {
        retryOnConflict(() -> {
            KafkaUser existing = kafkaUserOps.get(namespace, username);
            if (existing == null) {
                LOGGER.info("KafkaUser '{}' does not exist; skipping update.", username);
                return true;
            }

            KafkaUserBuilder builder = new KafkaUserBuilder(existing);

            // TODO: remove this?? dummy update
            builder.editOrNewMetadata().addToLabels("new-label", "" + new Random().nextInt(Integer.MAX_VALUE)).endMetadata();

            // Authentication
            if ("scramsha".equalsIgnoreCase(authType)) {
                builder
                    .editOrNewSpec()
                        .withNewKafkaUserScramSha512ClientAuthentication()
                        .endKafkaUserScramSha512ClientAuthentication()
                    .endSpec();
                // TODO: another flag to use custom scram sha password isntead of randon one )) and use this as code
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
            } else if ("tls".equalsIgnoreCase(authType)) {
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
                            .withConsumerByteRate(new Random().nextInt(1000) + 100)
                            .withProducerByteRate(new Random().nextInt(1000) + 200)
                            .withRequestPercentage(new Random().nextInt(100))
                            .withControllerMutationRate(Math.random() * 10)
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
            waitUntilKafkaUserReady(username, namespace, 15_000);

            return true;
        });
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

}
