package io.strimzi.operator.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.common.Password;
import io.strimzi.api.kafka.model.common.PasswordBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthenticationBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
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
import org.mockito.Mockito;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

// TODO: Issues found (not sure if these are real bugs or expected behavior):
// 1. When a user is created with authType 'none' and later updated to 'scramsha' or 'tls',
//    the corresponding Secret is not created.
// 2. In the trace sequence (e.g., traces2.json), repeated updates and deletes for user 'bob'
//    can lead to a timeout when trying to recreate 'bob' later.
//    → Results in: Timeout after 15000 ms waiting for KafkaUser bob to become Ready
public class UserControllerModelMbtTest {
    private static final Logger LOGGER = LogManager.getLogger(UserControllerModelMbtTest.class);
    private static final Random RANDOM = new Random();
    private static final int POLL_INTERVAL_MS = 100;

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private String namespace;
    private SecretOperator secretOperator;
    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    private static StrimziKafkaCluster kafkaCluster;
    private static Admin adminClient;
    private KafkaUserOperator kafkaUserOperator;
    private QuotasOperator quotasOperator;
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
        List<String> mbtTimeline = new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        InputStream input = getClass().getResourceAsStream(tracePath);
        Trace trace = mapper.readValue(input, Trace.class);

        // Prepare controller
        MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());
        UserController controller = new UserController(
            config,
            secretOperator,
            kafkaUserOps,
            kafkaUserOperator,
            metrics
        );
        InvariantChecker invariants = new InvariantChecker(kafkaUserOps, secretOperator);

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

                String stepInfo = String.format(
                    "MBT: [%d] Executing action '%s' for user='%s', authType='%s', quotasEnabled='%s'",
                    i, action, username, authType, quotasEnabled
                );LOGGER.info(stepInfo);
                mbtTimeline.add(stepInfo);

                // TODO: if none => secret is not created check that :))
                if (i > 0) {
                    switch (action) {
                        case "createUser" -> createKafkaUser(authType, quotasEnabled, username);
                        case "updateUser" -> updateKafkaUser(authType, quotasEnabled, username);
                        case "deleteUser" -> {
                            KafkaUser existing = kafkaUserOps.get(namespace, username);
                            if (existing != null) {
                                kafkaUserOps.resource(namespace, username).delete();
                                kafkaUserOps.resource(namespace, username)
                                    .waitUntilCondition(u -> u == null, 10_000, TimeUnit.MILLISECONDS);

                                // TODO: investigate why I have to delete manually
                                //     tried: i) remove finallizers orphans
                                //            ii) there is also ownerReference with the KU so maybe that's some kind or race condition...
                                Secret secret = secretOperator.get(namespace, username);
                                if (secret != null) {
                                    secretOperator.resource(namespace, username).delete();
                                }

                                waitUntilUserAndSecretDeleted(username, namespace, 15_000);
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
                invariants.assertSecretsConsistency(namespace);
                invariants.assertNoSecretsForDeletedUsers(namespace);
                invariants.assertQuotasNonNegative(namespace);
                invariants.assertQuotasRequestPercentageValid(namespace);
                invariants.assertReadyUsersQuotasValid(namespace);
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
            .build();
        mockKube.start();
        client = mockKube.client();

        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);

        // Wait until the namespace is truly gone
        mockKube.prepareNamespace(namespace);

        config = ResourceUtils.createUserOperatorConfigForUserControllerTesting(
            namespace,
            Map.of(),
            1000,
            100,          // Batch queue size
            10,                     // Max batch size
            "",                     // Optional secret prefix
            1                       // controller thread size
        );

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

        ScramCredentialsOperator scramCredentialsOperator = new ScramCredentialsOperator(adminClient, config, ForkJoinPool.commonPool());

        // Without calling start(), the QuotasOperator would not initialize its cache nor trigger periodic quota refreshes → reconciliations would fail or work incorrectly.
        quotasOperator = new QuotasOperator(adminClient, config, ForkJoinPool.commonPool());

        // TODO: use real ACL operator instead of mock :)
        SimpleAclOperator aclOperator = Mockito.mock(SimpleAclOperator.class);
        when(aclOperator.reconcile(any(), any(), any()))
            .thenAnswer(i -> CompletableFuture.supplyAsync(() -> {
                try {
                    // introduce small async delays to your mocked operators to avoid lock starvation
                    Thread.sleep(50 + RANDOM.nextInt(100));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }));

        CertManager certManager = new MockCertManager();

        kafkaUserOperator = new KafkaUserOperator(
            config,
            certManager,
            secretOperator,
            kafkaUserOps,
            scramCredentialsOperator,
            quotasOperator,
            aclOperator
        );
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
            Set<String> secretNames = secretOperator.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .map(secret -> secret.getMetadata().getName())
                .collect(Collectors.toSet());

            Set<String> userNames = kafkaUserOps.list(namespace, InvariantChecker.kafkaUserLabels).stream()
                .map(user -> user.getMetadata().getName())
                .collect(Collectors.toSet());

            boolean allSecretsHaveUsers = secretNames.stream().allMatch(userNames::contains);

            if (allSecretsHaveUsers) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        throw new RuntimeException("Timeout waiting for Secrets without corresponding KafkaUsers to be deleted");
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

    private void waitUntilUserAndSecretDeleted(String username, String namespace, long timeoutMillis) {
        TestUtils.waitFor(
            "KafkaUser " + username + " and Secret to be deleted",
            Duration.ofMillis(POLL_INTERVAL_MS).toMillis(),
            Duration.ofMillis(timeoutMillis).toMillis(),
            () -> {
                boolean userDeleted = kafkaUserOps.get(namespace, username) == null;
                boolean secretDeleted = secretOperator.get(namespace, username) == null;
                return userDeleted && secretDeleted;
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

    private void createKafkaUser(String authType, Boolean quotasEnabled, String username) throws Exception {
        try {
            if ("tls".equalsIgnoreCase(authType)) {
                if (Boolean.TRUE.equals(quotasEnabled)) {
                    kafkaUserOps.resource(namespace,
                            ResourceUtils.createKafkaUserWithQuotas(namespace, username,
                                new KafkaUserQuotasBuilder()
                                    .withConsumerByteRate(100)
                                    .withProducerByteRate(200)
                                    .withRequestPercentage(50)
                                    .withControllerMutationRate(5.0)
                                    .build()))
                        .create();
                } else {
                    kafkaUserOps.resource(namespace,
                            ResourceUtils.createKafkaUserTls(namespace, username))
                        .create();
                }
            } else if ("scramsha".equalsIgnoreCase(authType)) {
                if (Boolean.TRUE.equals(quotasEnabled)) {
                    kafkaUserOps.resource(namespace,
                            ResourceUtils.createKafkaUserScramShaAndQuotas(namespace, username,
                                new KafkaUserQuotasBuilder()
                                    .withConsumerByteRate(100)
                                    .withProducerByteRate(200)
                                    .withRequestPercentage(50)
                                    .withControllerMutationRate(5.0)
                                    .build()))
                        .create();
                } else {
                    kafkaUserOps.resource(namespace, ResourceUtils.createKafkaUserScramSha(namespace, username))
                        .create();
                }
            } else {
                // No auth type (NONE)

                if (Boolean.TRUE.equals(quotasEnabled)) {
                    kafkaUserOps.resource(namespace,
                       new KafkaUserBuilder()
                            .withNewMetadata()
                                .withLabels(Labels.forStrimziCluster("my-cluster").toMap())
                                .withName(username)
                                .withNamespace(namespace)
                            .endMetadata()
                            .withNewSpec()
                                .withQuotas(new KafkaUserQuotasBuilder()
                                   .withConsumerByteRate(100)
                                   .withProducerByteRate(200)
                                   .withRequestPercentage(50)
                                   .withControllerMutationRate(5.0)
                                   .build())
                           .withAuthentication(new KafkaUserTlsClientAuthentication())
                            .endSpec()
                           .build())
                        .create();
                } else {
                    kafkaUserOps.resource(namespace,
                        new KafkaUserBuilder()
                            .withNewMetadata()
                            .withLabels(Labels.forStrimziCluster("my-cluster").toMap())
                            .withName(username)
                            .withNamespace(namespace)
                            .endMetadata()
                            .withNewSpec()
                            .endSpec()
                            .build())
                        .create();
                }
            }
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

    private void updateKafkaUser(String authType, Boolean quotasEnabled, String username) throws Exception {
        KafkaUser existing = kafkaUserOps.get(namespace, username);
        if (existing == null) {
            LOGGER.info("KafkaUser '{}' does not exist; skipping update.", username);
            return;
        }

        if ("tls".equalsIgnoreCase(authType)) {
            if (Boolean.TRUE.equals(quotasEnabled)) {
                existing.getSpec().setQuotas(
                        new KafkaUserQuotasBuilder()
                            .withConsumerByteRate(500)
                            .withProducerByteRate(500)
                            .withRequestPercentage(90)
                            .withControllerMutationRate(20.0)
                            .build());
                existing.getSpec().setAuthentication((new KafkaUserTlsClientAuthentication()));
            } else {
                existing.getSpec().setAdditionalProperty("roles", List.of("admin"));
                existing.getSpec().setAuthentication((new KafkaUserTlsClientAuthentication()));

            }
        } else if ("scramsha".equalsIgnoreCase(authType)) {
            if (Boolean.TRUE.equals(quotasEnabled)) {
                // when also Quotas we also update custom scram-sha
                final String secretName = "custom-secret-scram-sha";
                final Secret userDefinedSecret = new SecretBuilder()
                    .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                    .endMetadata()
                    .addToData("password", "VDZmQ2pNMWRRb1d6VnBYNWJHa1VSOGVOMmFIeFA3WXM=")
                    .build();

                client.resource(userDefinedSecret).create();

                existing.getSpec().setQuotas(
                        new KafkaUserQuotasBuilder()
                            .withConsumerByteRate(332)
                            .withProducerByteRate(522)
                            .withRequestPercentage(94)
                            .withControllerMutationRate(22.0)
                            .build());
                existing.getSpec().setAuthentication(
                    new KafkaUserScramSha512ClientAuthenticationBuilder()
                        .withPassword(new PasswordBuilder()
                            .editOrNewValueFrom()
                                .withNewSecretKeyRef("password", secretName, false)
                            .endValueFrom()
                            .build())
                        .build()
                );
            } else {
                existing.getSpec().setAuthentication((new KafkaUserScramSha512ClientAuthentication()));
                // For SCRAM users, you might update quotas or add a dummy field
                existing.getSpec().setAdditionalProperty("dummyUpdate", "true");
            }
         } else {
            // none
            if (Boolean.TRUE.equals(quotasEnabled)) {
                existing.getSpec().setQuotas(new KafkaUserQuotasBuilder()
                    .withConsumerByteRate(100)
                    .withProducerByteRate(200)
                    .withRequestPercentage(50)
                    .withControllerMutationRate(5.0)
                    .build());
                existing.getSpec().setAuthentication(null);
            } else {
                existing.getSpec().setAuthentication(null);
            }
        }
        kafkaUserOps.resource(namespace, existing).update();
        waitUntilKafkaUserReady(username, namespace, 15_000);
    }

}
