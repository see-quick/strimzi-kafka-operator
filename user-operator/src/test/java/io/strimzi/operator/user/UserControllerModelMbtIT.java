package io.strimzi.operator.user;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import io.strimzi.operator.user.operator.DisabledSimpleAclOperator;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import io.strimzi.operator.user.operator.QuotasOperator;
import io.strimzi.operator.user.operator.ScramCredentialsOperator;
import io.strimzi.operator.user.operator.SimpleAclOperator;
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
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

/**
 * Model-Based Test (MBT) for the Strimzi User Operator.
 *
 * This test replays formal execution traces (JSON files) generated from a Quint specification
 * (`UserOperatorModel.qnt`) and validates that the real Java User Operator behaves as the model expects.
 *
 * Each trace describes a sequence of actions (createUser, updateUser, deleteUser, processNextEvent).
 * - Actions are queued until `processNextEvent` is explicitly requested, mimicking the model's event queue.
 * - After each step, invariants (user/secret/ACL/quotas consistency) are checked against the real system.
 *
 * Components under test include:
 * - KafkaUser creation/update/deletion
 * - Secret handling based on authentication
 * - ACLs based on authorization
 * - Quotas validation
 *
 * This test ensures that the real implementation honors the same invariants and properties as the Quint model.
 */
public class UserControllerModelMbtIT {
    private static final Logger LOGGER = LogManager.getLogger(UserControllerModelMbtIT.class);

    private static MockKube3 mockKube;
    private static StrimziKafkaCluster kafkaCluster;
    private static Admin adminClient;

    private CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps;
    private SecretOperator secretOperator;
    private String namespace;

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Trace {
        public List<Map<String, Object>> states;
    }

    static Stream<String> testCaseProvider() {
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
    @MethodSource("testCaseProvider")
    public void testUserOperator(String tracePath) throws Exception {
        final List<String> mbtTimeline = new ArrayList<>();

        final ObjectMapper mapper = new ObjectMapper();
        final InputStream input = getClass().getResourceAsStream(tracePath);
        final Trace testCase = mapper.readValue(input, Trace.class);

        // assumption is that trace would have always more than 0 states
        final Map<String, Object> parameters = (Map<String, Object>) testCase.states.get(0).get("parameters");

        final Boolean aclsEnabled = (Boolean) parameters.get("aclsEnabled");
        final List<String> usersToTest = (List<String>) ((Map<String, Object>) parameters.get("potentialUsers")).get("#set");
        final Integer maxProcessedEvents = Integer.parseInt((String) ((Map<String, Object>) parameters.get("maxProcessedEvents")).get("#bigint"));

        LOGGER.info("📋 STARTING TEST CASE: {}", tracePath);
        LOGGER.info("====================");
        LOGGER.info("1️⃣ PARAMETER aclEnabled = {}", aclsEnabled);
        LOGGER.info("2️⃣ PARAMETER potentialUsers = {}", usersToTest);
        LOGGER.info("3️⃣ PARAMETER maxProcessedEvents = {}", maxProcessedEvents);

        final UserOperatorConfig config = ResourceUtils.createUserOperatorConfigForUserControllerTesting(
            namespace,
            Map.of(),
            1000,
            100,          // Batch queue size
            1,                      // Max batch size
            "",                     // Optional secret prefix
            1,                      // controller thread size
            aclsEnabled             // ACLs enabled/disabled
        );

        LOGGER.info("Starting with config: {}", config.toString());

        final ScramCredentialsOperator scramCredentialsOperator = new ScramCredentialsOperator(adminClient, config, ForkJoinPool.commonPool());
        final QuotasOperator quotasOperator = new QuotasOperator(adminClient, config, ForkJoinPool.commonPool());
        final CertManager certManager = new MockCertManager();
        final MetricsProvider metrics = new MicrometerMetricsProvider(new SimpleMeterRegistry());
        final KafkaUserOperator kafkaUserOperator = new KafkaUserOperator(
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
        final UserController controller = new UserController(
            config,
            secretOperator,
            kafkaUserOps,
            kafkaUserOperator,
            metrics
        );

        kafkaUserOperator.start();
        controller.start();

        final KafkaUserModelActions actions = new KafkaUserModelActions(kafkaUserOps, secretOperator, namespace);
        final List<KafkaUserModelActions.ModelEvent> eventQueue = new ArrayList<>();

        try {
            for (int i = 0; i < testCase.states.size(); i++) {
                final Map<String, Object> state = testCase.states.get(i);
                final Map<String, Object> globalState = (Map<String, Object>) state.get("globalState");
                final int processedEvents = Integer.parseInt(
                    String.valueOf(((Map<String, Object>) globalState.get("processedEvents")).get("#bigint"))
                );

                if (processedEvents >= maxProcessedEvents) {
                    LOGGER.info("🍀 Reached maxProcessedEvents={} at step {} — finishing early.", maxProcessedEvents, i);
                    break;
                }

                final String action = (String) state.get("mbt::actionTaken");

                String username = null;
                String authType = null;
                Boolean quotasEnabled = null;
                String resourceType = null;
                String patternType = null;
                String operation = null;
                Boolean reconciliationPaused = null;

                final Map<String, Object> nondet = (Map<String, Object>) state.get("mbt::nondetPicks");
                if (nondet != null) {
                    username = ResourceUtils.getOptionalValue(nondet, "u", String.class);
                    if (username != null) {
                        username = username.toLowerCase(Locale.ROOT);
                    }
                    authType = ResourceUtils.getOptionalValue(nondet, "authType", String.class);
                    quotasEnabled = ResourceUtils.getOptionalValue(nondet, "quotasEnabled", Boolean.class);

                    final List<String> authzParams = ResourceUtils.getTupleEnumTags(nondet, "authzParams", 3);

                    if (authzParams.size() == 3) {
                        resourceType = authzParams.get(0);
                        patternType = authzParams.get(1);
                        operation = authzParams.get(2);
                    }

                    reconciliationPaused = ResourceUtils.getOptionalValue(nondet, "reconciliationPaused", Boolean.class);
                }

                final InvariantChecker invariants = new InvariantChecker(kafkaUserOps, secretOperator);

                final String stepInfo = String.format("""
                    ⊧ MBT: [%d] %s action='%s'
                        ├─ user='%s'
                        ├─ authType='%s'
                        ├─ quotasEnabled='%s'
                        ├─ aclsEnabled='%s'
                        ├─ resourceType='%s'
                        ├─ pattern='%s'
                        ├─ operation='%s'
                        └─ reconciliationPaused='%s'
                    """,
                    i,
                    "processNextEvent".equals(action) ? "Executing" : "Queueing",
                    action,
                    username,
                    authType,
                    quotasEnabled,
                    aclsEnabled,
                    resourceType,
                    patternType,
                    operation,
                    reconciliationPaused
                );
                LOGGER.debug(stepInfo);
                mbtTimeline.add(stepInfo);

                // i = 0 is init state
                if (i > 0) {
                    final KafkaUserModelActions.Action act = KafkaUserModelActions.Action.fromString(action);
                    switch (act) {
                        case CREATE_USER ->
                            eventQueue.add(
                                KafkaUserModelActions.EventsFactory.create(
                                    username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation, reconciliationPaused));
                        case UPDATE_USER ->
                            eventQueue.add(
                                KafkaUserModelActions.EventsFactory.update(
                                    username, authType, quotasEnabled, aclsEnabled, resourceType, patternType, operation, reconciliationPaused));
                        case DELETE_USER ->
                            eventQueue.add(
                                KafkaUserModelActions.EventsFactory.delete(username, authType));
                        case PROCESS_NEXT_EVENT -> {
                            actions.processNextEvent(eventQueue);

                            //Every actual secret must correspond to a user with authType != 'none'
                            actions.waitUntilAllSecretsHaveMatchingKafkaUsers(namespace);

                            invariants.assertControllerAlive(controller);
                            invariants.assertControllerReady(controller);
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
                            invariants.assertReadyUsersMustHaveACLs(namespace);
                        }
                        default -> { /* no-op */ }
                    }
                }
            }
        } finally {
            kafkaUserOperator.stop();
            controller.stop();
            LOGGER.info("ℹ️ MBT test trace for {}. Timeline of actions:", tracePath);
            mbtTimeline.forEach(step -> LOGGER.debug(step));
        }
    }

    @BeforeAll
    public static void beforeAll() {
        final Map<String, String> additionalConfiguration = Map.of(
            "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "super.users", "User:ANONYMOUS");

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withSharedNetwork()
            .withAdditionalKafkaConfiguration(additionalConfiguration)
            .build();
        kafkaCluster.start();

        ResourceUtils.waitUntilKafkaReady(kafkaCluster.getBootstrapServers(), Duration.ofSeconds(10));

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        adminClient = AdminClient.create(props);
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaUserCrd()
            .withDeletionController()
            .build();
        mockKube.start();

        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);

        // Wait until the namespace is truly gone
        mockKube.prepareNamespace(namespace);

        final KubernetesClient client = mockKube.client();
        secretOperator = new SecretOperator(ForkJoinPool.commonPool(), client);

        // Create dummy ca-cert Secret required by KafkaUserOperator
        final Secret caCert = new SecretBuilder()
            .withNewMetadata()
                .withName(ResourceUtils.CA_CERT_NAME)
                .withNamespace(namespace)
                .addToAnnotations("strimzi.io/ca-cert-generation", "1")
            .endMetadata()
            .withData(Map.of("ca.crt", "ZHVtbXk=")) // "dummy" base64-encoded
            .build();

        // Create dummy ca-key Secret required by KafkaUserOperator
        final Secret caKey = new SecretBuilder()
            .withNewMetadata()
                .withName(ResourceUtils.CA_KEY_NAME)
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
    void afterEach() {
        if (mockKube != null) {
            mockKube.stop();
        }
    }

    @AfterAll
    public static void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaCluster != null) {
            kafkaCluster.stop();
        }
    }
}
