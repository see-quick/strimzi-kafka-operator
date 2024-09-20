/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.featuregates;

import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.Value;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ParallelSuite
public class FeatureGatesTest {
    @ParallelTest
    public void testIndividualFeatureGates() {
        for (FeatureGates.FeatureGate gate : FeatureGates.NONE.allFeatureGates()) {
            FeatureGates enabled = FeatureGates.getInstance("+" + gate.getName());
            FeatureGates disabled = FeatureGates.getInstance("-" + gate.getName());

            assertThat(enabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(true));
            assertThat(disabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(false));
        }
    }

    @ParallelTest
    public void testAllFeatureGates() {
        List<String> allEnabled = new ArrayList<>();
        List<String> allDisabled = new ArrayList<>();

        for (FeatureGates.FeatureGate gate : FeatureGates.NONE.allFeatureGates()) {
            allEnabled.add("+" + gate.getName());
            allDisabled.add("-" + gate.getName());
        }

        FeatureGates enabled = FeatureGates.getInstance(String.join(",", allEnabled));
        for (FeatureGates.FeatureGate gate : enabled.allFeatureGates()) {
            assertThat(gate.isEnabled(), is(true));
        }

        FeatureGates disabled = FeatureGates.getInstance(String.join(",", allDisabled));
        for (FeatureGates.FeatureGate gate : disabled.allFeatureGates()) {
            assertThat(gate.isEnabled(), is(false));
        }
    }

    @ParallelTest
    public void testFeatureGatesParsing() {
        assertThat(FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(true));
        assertThat(FeatureGates.getInstance("-ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(false));
        assertThat(FeatureGates.getInstance("  -ContinueReconciliationOnManualRollingUpdateFailure    ").continueOnManualRUFailureEnabled(), is(false));
        // TODO: Add more tests with various feature gate combinations once we have multiple feature gates again.
        //       The commented out code below shows the tests we used to have with multiple feature gates.
        //assertThat(FeatureGates.getInstance("-UseKRaft,-ContinueReconciliationOnManualRollingUpdateFailure").useKRaftEnabled(), is(false));
        //assertThat(FeatureGates.getInstance("-UseKRaft,-ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(false));
        //assertThat(FeatureGates.getInstance("  +UseKRaft    ,    +ContinueReconciliationOnManualRollingUpdateFailure").useKRaftEnabled(), is(true));
        //assertThat(FeatureGates.getInstance("  +UseKRaft    ,    +ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(true));
        //assertThat(FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure,-UseKRaft").useKRaftEnabled(), is(false));
        //assertThat(FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure,-UseKRaft").continueOnManualRUFailureEnabled(), is(true));
    }

    @ParallelTest
    public void testFeatureGatesEquals() {
        FeatureGates fg = FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure");
        assertThat(fg, is(fg));
        assertThat(fg, is(FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure")));
        assertThat(fg, is(not(FeatureGates.getInstance("-ContinueReconciliationOnManualRollingUpdateFailure"))));
    }

    @ParallelTest
    public void testEmptyFeatureGates() {
        List<FeatureGates> emptyFeatureGates = List.of(
                FeatureGates.getInstance(null),
                FeatureGates.getInstance(""),
                FeatureGates.getInstance("  "),
                FeatureGates.getInstance("    "),
                FeatureGates.NONE);

        for (FeatureGates fgs : emptyFeatureGates)  {
            for (FeatureGates.FeatureGate fg : fgs.allFeatureGates()) {
                assertThat(fg.isEnabled(), is(fg.isEnabledByDefault()));
            }
        }
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithSameValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure,+ContinueReconciliationOnManualRollingUpdateFailure"));
        assertThat(e.getMessage(), containsString("Feature gate ContinueReconciliationOnManualRollingUpdateFailure is configured multiple times"));
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithDifferentValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure,-ContinueReconciliationOnManualRollingUpdateFailure"));
        assertThat(e.getMessage(), containsString("Feature gate ContinueReconciliationOnManualRollingUpdateFailure is configured multiple times"));
    }

    @ParallelTest
    public void testMissingSign() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> FeatureGates.getInstance("ContinueReconciliationOnManualRollingUpdateFailure"));
        assertThat(e.getMessage(), containsString("ContinueReconciliationOnManualRollingUpdateFailure is not a valid feature gate configuration"));
    }

    @ParallelTest
    public void testNonExistingGate() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> FeatureGates.getInstance("+RandomGate"));
        assertThat(e.getMessage(), containsString("Unknown feature gate RandomGate found in the configuration"));
    }

    @ParallelTest
    public void testEnvironmentVariable()   {
        assertThat(FeatureGates.getInstance("").toEnvironmentVariable(), is(""));
        assertThat(FeatureGates.getInstance("-ContinueReconciliationOnManualRollingUpdateFailure").toEnvironmentVariable(), is("-ContinueReconciliationOnManualRollingUpdateFailure"));
        assertThat(FeatureGates.getInstance("+ContinueReconciliationOnManualRollingUpdateFailure").toEnvironmentVariable(), is(""));
    }

    @ParallelTest
    void testFeatureFlagWithDifferentContexts() {
        // Mock the isEnvVarProvider method to return false
        FeatureGates spyFeatureGates = Mockito.spy(FeatureGates.getInstance(""));
        doReturn(false).when(spyFeatureGates).isEnvVarProvider();

        // Create different EvaluationContexts
        EvaluationContext contextA = mock(EvaluationContext.class);
        when(contextA.getValue("clusterName")).thenReturn(new Value("kafka-cluster-a"));
        when(contextA.getValue("namespace")).thenReturn(new Value("namespace-a"));

        EvaluationContext contextB = mock(EvaluationContext.class);
        when(contextB.getValue("clusterName")).thenReturn(new Value("kafka-cluster-c"));
        when(contextB.getValue("namespace")).thenReturn(new Value("namespace-c"));

        // Simulate flag values based on context
        doReturn(true).when(spyFeatureGates)
            .fetchFeatureFlag(eq("ContinueReconciliationOnManualRollingUpdateFailure"), eq(true), eq(Boolean.class), eq(contextA));
        doReturn(false).when(spyFeatureGates)
            .fetchFeatureFlag(eq("ContinueReconciliationOnManualRollingUpdateFailure"), eq(true), eq(Boolean.class), eq(contextB));

        // Test with contextA where the flag should be enabled
        spyFeatureGates.maybeUpdateFeatureGateStatesOfKafka(contextA);
        assertTrue(spyFeatureGates.continueOnManualRUFailureEnabled(), "Feature should be enabled for contextA.");

        // Test with contextB where the flag should be disabled
        spyFeatureGates.maybeUpdateFeatureGateStatesOfKafka(contextB);
        assertFalse(spyFeatureGates.continueOnManualRUFailureEnabled(), "Feature should be disabled for contextB.");
    }

}
