package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.concurrent.CrdOperator;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A test-only {@code CrdOperator} for {@code KafkaUser} that injects faults (409, 404, 500, pause).

 * Used in model-based tests to simulate reconciliation errors and delays (which are hard to simulate normally).
 * Faults auto-reset after each invocation.
 */
public class FaultyCrdOperator extends CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> {

    private boolean simulatePause;
    private boolean simulateConflict;
    private boolean simulateGone;
    private boolean simulateServerError;

    /**
     * Private constructor. Use factory method {@link #create(Executor, KubernetesClient)} instead.
     */
    private FaultyCrdOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, KafkaUser.class, KafkaUserList.class, "KafkaUser");
    }

    public static FaultyCrdOperator create(Executor asyncExecutor, KubernetesClient client) {
        return new FaultyCrdOperator(asyncExecutor, client);
    }

    public void enablePause() {
        this.simulatePause = true;
    }

    public void enableConflict() {
        this.simulateConflict = true;
    }

    public void enableGone() {
        this.simulateGone = true;
    }

    public void enableServerError() {
        this.simulateServerError = true;
    }

    public void disableAllFaults() {
        this.simulatePause = false;
        this.simulateConflict = false;
        this.simulateGone = false;
        this.simulateServerError = false;
    }

    @Override
    public CompletionStage<ReconcileResult<KafkaUser>> createOrUpdate(Reconciliation reconciliation, KafkaUser resource) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                simulateFaults("createOrUpdate");
                return ReconcileResult.created(operation().inNamespace(resource.getMetadata().getNamespace()).resource(resource).createOrReplace());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                disableAllFaults();
            }
        }, asyncExecutor);
    }

    @Override
    public CompletionStage<KafkaUser> updateStatusAsync(Reconciliation reconciliation, KafkaUser resource) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                simulateFaults("updateStatusAsync");
                return operation().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                disableAllFaults();
            }
        }, asyncExecutor);
    }

    /**
     * Injects the currently enabled faults during reconciliation.
     *
     * @param   method name of the method calling this logic (used in error messages)
     * @throws  InterruptedException if pause is injected
     */
    private void simulateFaults(String method) throws InterruptedException {
        if (this.simulatePause) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(50, 200));
        }
        if (this.simulateConflict) {
            throw mockKubeClientException(409, "Simulated conflict in " + method);
        }
        if (this.simulateGone) {
            throw mockKubeClientException(404, "Simulated 404 Not Found in " + method);
        }
        if (this.simulateServerError) {
            throw mockKubeClientException(500, "Simulated 500 Internal Server Error in " + method);
        }
    }

    /**
     * Creates a fake {@link KubernetesClientException} with the given status code and message.
     *
     * @param code      HTTP-like error code
     * @param message   description of the simulated error
     * @return a simulated {@link KubernetesClientException}
     */
    private KubernetesClientException mockKubeClientException(int code, String message) {
        return new KubernetesClientException(message, code,
            new StatusBuilder()
                .withCode(code)
                .withMessage(message)
                .build()
        );
    }
}