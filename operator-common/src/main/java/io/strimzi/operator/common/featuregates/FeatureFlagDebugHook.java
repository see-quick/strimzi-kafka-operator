/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.featuregates;

import dev.openfeature.sdk.BooleanHook;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FlagEvaluationDetails;
import dev.openfeature.sdk.FlagValueType;
import dev.openfeature.sdk.HookContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * A custom OpenFeature hook that provides logging for debugging the evaluation
 * process of feature flags in a boolean context.
 *
 * <p>This hook logs information before and after the evaluation of a feature flag,
 * as well as in case of errors and after finalization.
 */
public class FeatureFlagDebugHook implements BooleanHook {

    private final static Logger LOGGER = LogManager.getLogger(FeatureFlagDebugHook.class);

    /**
     * Determines if this hook supports the specified flag value type.
     *
     * @param flagValueType the type of the flag value being evaluated
     * @return {@code true} if the flag value type is supported, otherwise {@code false}
     */
    @Override
    public boolean supportsFlagValueType(FlagValueType flagValueType) {
        return BooleanHook.super.supportsFlagValueType(flagValueType);
    }

    /**
     * Executes before the flag evaluation. Logs the flag key, default value, context, and hints.
     *
     * @param ctx the evaluation context
     * @param hints additional information provided to the hook
     * @return an optional modified evaluation context
     */
    @Override
    public Optional<EvaluationContext> before(HookContext<Boolean> ctx, Map<String, Object> hints) {
        LOGGER.info("Before evaluation of flag: {}\n with default value: {}\n, and hits: {}\n and whole context: {}\n",
            ctx.getFlagKey(),
            ctx.getDefaultValue(),
            hints.toString(),
            ctx.toString());
        return BooleanHook.super.before(ctx, hints);
    }

    /**
     * Executes after the flag evaluation. Logs the flag key, default value, context, hints, and evaluation details.
     *
     * @param ctx the evaluation context
     * @param details the evaluation details of the flag
     * @param hints additional information provided to the hook
     */
    @Override
    public void after(HookContext<Boolean> ctx, FlagEvaluationDetails<Boolean> details, Map<String, Object> hints) {
        LOGGER.info("After evaluation of flag: {}\n with default value: {}\n, and hits: {}\n and whole context: {}\n and details: {}\n",
            ctx.getFlagKey(),
            ctx.getDefaultValue(),
            hints.toString(),
            ctx.toString(),
            details.toString());
        BooleanHook.super.after(ctx, details, hints);
    }

    /**
     * Handles errors that occur during the flag evaluation process.
     * Logs the error details along with the flag key, default value, and context.
     *
     * @param ctx the evaluation context
     * @param error the exception that occurred
     * @param hints additional information provided to the hook
     */
    @Override
    public void error(HookContext<Boolean> ctx, Exception error, Map<String, Object> hints) {
        LOGGER.error("Error state of flag: {}\n with default value: {}\n, and hits: {}\n and whole context: {}\n",
            ctx.getFlagKey(),
            ctx.getDefaultValue(),
            hints.toString(),
            ctx.toString());
        BooleanHook.super.error(ctx, error, hints);
    }

    /**
     * Executes after the entire flag evaluation process, regardless of success or failure.
     * Logs the final state of the evaluation.
     *
     * @param ctx the evaluation context
     * @param hints additional information provided to the hook
     */
    @Override
    public void finallyAfter(HookContext<Boolean> ctx, Map<String, Object> hints) {
        LOGGER.info("Finally always executes flag: {}\n with default value: {}\n, and hits: {}\n and whole context: {}\n",
            ctx.getFlagKey(),
            ctx.getDefaultValue(),
            hints.toString(),
            ctx.toString());
        BooleanHook.super.finallyAfter(ctx, hints);
    }
}
