/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.exceptions;

/**
 * Provides a handler in {@link @BeforeAll}, when Kubernetes API is not alive it throws this exception <b>before</b>
 * test cases will fail on {@link io.strimzi.operator.common.operator.resource.TimeoutException}.
 */
public class ClusterApiNotHealthyException extends RuntimeException {

    /**
     * ClusterNotHealthyException used for handling situation, when Kubernetes API is not alive it throws this
     * exception <b>before</b> test cases will fail on {@link io.strimzi.operator.common.operator.resource.TimeoutException}.
     *
     * @param message specific message to throw
     */
    public ClusterApiNotHealthyException(String message) {
        super(message);
    }

    /**
     * ClusterNotHealthyException used for handling situation, when Kubernetes API is not alive it throws this
     * exception <b>before</b> test cases will fail on {@link io.strimzi.operator.common.operator.resource.TimeoutException}.
     *
     * @param cause specific cause to throw
     */
    public ClusterApiNotHealthyException(Throwable cause) {
        super(cause);
    }
}