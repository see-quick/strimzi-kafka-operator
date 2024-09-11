/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.featuregates;

import dev.openfeature.contrib.providers.envvar.EnvVarProvider;
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.FeatureProvider;
import dev.openfeature.sdk.OpenFeatureAPI;
import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

/**
 * Class for handling the configuration of feature gates
 */
public class FeatureGates {
    /* test */ static final FeatureGates NONE = new FeatureGates("");

    private static final String CONTINUE_ON_MANUAL_RU_FAILURE = "ContinueReconciliationOnManualRollingUpdateFailure";
    private static final String FLAGD_ENABLED_ENV_VAR = "FLAGD_ENABLED"; // Environment variable to toggle FlagD

    private final Client featureClient;
    private final FeatureProvider provider;

    // When adding new feature gates, do not forget to add them to allFeatureGates(), toString(), equals(), and `hashCode() methods
    private FeatureGate continueOnManualRUFailure;

    /**
     * Constructs the feature gates configuration.
     *
     * @param featureGateConfig String with a comma-separated list of enabled or disabled feature gates
     */
    public FeatureGates(String featureGateConfig) {
        // Set the appropriate provider based on the environment variable
        this.provider = isFlagDEnabled() ? new FlagdProvider() : new EnvVarProvider();
        OpenFeatureAPI.getInstance().setProvider(this.provider);
        this.featureClient = OpenFeatureAPI.getInstance().getClient();

        // Validate and parse the featureGateConfig if it's provided
        if (featureGateConfig != null && !featureGateConfig.trim().isEmpty()) {
            List<String> featureGates;

            // Validate the format of the feature gate configuration string
            if (featureGateConfig.matches("(\\s*[+-][a-zA-Z0-9]+\\s*,)*\\s*[+-][a-zA-Z0-9]+\\s*")) {
                featureGates = asList(featureGateConfig.trim().split("\\s*,+\\s*"));
            } else {
                throw new InvalidConfigurationException(featureGateConfig + " is not a valid feature gate configuration");
            }

            // Validate each feature gate in the config to ensure it is recognized
            for (String featureGate : featureGates) {
                featureGate = featureGate.substring(1); // Remove the + or - sign

                // Only validate feature gates but do not apply them manually
                switch (featureGate) {
                    case CONTINUE_ON_MANUAL_RU_FAILURE:
                        // This is a valid feature gate; continue with processing
                        break;
                    default:
                        throw new InvalidConfigurationException("Unknown feature gate " + featureGate + " found in the configuration");
                }
            }
        }

        // Fetch feature gates using OpenFeature
        boolean continueOnManualRUFailureValue = fetchFeatureFlag(CONTINUE_ON_MANUAL_RU_FAILURE, true, Boolean.class);
        setValueOnlyOnce(continueOnManualRUFailure, continueOnManualRUFailureValue);

        // Validate interdependencies (if any)
        validateInterDependencies();
    }

    /**
     * Validates any dependencies between various feature gates. When the dependencies are not satisfied,
     * InvalidConfigurationException is thrown.
     */
    private void validateInterDependencies()    {
        // There are currently no interdependencies between different feature gates.
        // But we keep this method as these might happen again in the future.
    }

    /**
     * Checks whether FlagD is enabled via environment variables.
     *
     * @return True if FLAGD_ENABLED is set to "true", otherwise false.
     */
    public boolean isFlagDEnabled() {
        String flagDEnabled = System.getenv(FLAGD_ENABLED_ENV_VAR);
        return flagDEnabled != null && flagDEnabled.equalsIgnoreCase("true");
    }

    /**
     * Fetches the feature flag using OpenFeature and applies a default value if not present.
     *
     * @param flagName     The name of the feature flag
     * @param defaultValue The default value if the flag isn't set
     * @param <T>          The type of the feature flag (Boolean, String, Integer, etc.)
     * @param returnType   The class of the return type for determining which get method to call
     * @return The value of the feature flag
     */
    public <T> T fetchFeatureFlag(String flagName, T defaultValue, Class<T> returnType) {
        try {
            // Handle different types based on returnType
            if (returnType == Boolean.class) {
                return returnType.cast(featureClient.getBooleanValue(flagName, (Boolean) defaultValue));
            } else if (returnType == String.class) {
                return returnType.cast(featureClient.getStringValue(flagName, (String) defaultValue));
            } else if (returnType == Integer.class) {
                return returnType.cast(featureClient.getIntegerValue(flagName, (Integer) defaultValue));
            } else if (returnType == Double.class) {
                return returnType.cast(featureClient.getDoubleValue(flagName, (Double) defaultValue));
            } else {
                throw new IllegalArgumentException("Unsupported feature flag type: " + returnType.getSimpleName());
            }
        } catch (Exception e) {
            // Fallback in case of any issues fetching the flag
            return defaultValue;
        }
    }

    /**
     * Fetches and updates the feature gates state dynamically from the OpenFeature API.
     */
    public void updateFeatureGateStates() {
        if (isFlagDEnabled()) {
            // Fetch dynamically from FlagD and update internal states
            this.continueOnManualRUFailure.setValue(fetchFeatureFlag(CONTINUE_ON_MANUAL_RU_FAILURE, true, Boolean.class));
        } else {
            this.continueOnManualRUFailure.setValue(continueOnManualRUFailureEnabled());
            // Fallback to static configuration if FlagD is not enabled
        }
    }

    /**
     * Sets the feature gate value if it was not set yet. But if it is already set, then it throws an exception. This
     * helps to ensure that each feature gate is configured always only once.
     *
     * @param gate  Feature gate which is being configured
     * @param value Value which should be set
     */
    private void setValueOnlyOnce(FeatureGate gate, boolean value) {
        if (gate.isSet()) {
            throw new InvalidConfigurationException("Feature gate " + gate.getName() + " is configured multiple times");
        }

        gate.setValue(value);
    }

    /**
     * @return  Returns true when the ContinueReconciliationOnManualRollingUpdateFailure feature gate is enabled
     */
    public boolean continueOnManualRUFailureEnabled() {
        return continueOnManualRUFailure.isEnabled();
    }

    /**
     * Returns a list of all Feature gates. Used for testing.
     *
     * @return  List of all Feature Gates
     */
    /*test*/ List<FeatureGate> allFeatureGates()  {
        return List.of(continueOnManualRUFailure);
    }

    @Override
    public String toString() {
        return "FeatureGates(" +
                "ContinueReconciliationOnManualRollingUpdateFailure=" + continueOnManualRUFailure.isEnabled() +
                ")";
    }

    /**
     * @return  Generates the value for the environment variable that can be passed to the other operators to configure
     *          the feature gates exactly as they are set in this instance.
     */
    public String toEnvironmentVariable()   {
        List<String> gateSettings = new ArrayList<>();

        for (FeatureGate gate : allFeatureGates())  {
            if (gate.isEnabledByDefault() && !gate.isEnabled()) {
                // It is enabled by default but it is disabled now => we need to disable it
                gateSettings.add("-" + gate.getName());
            } else if (!gate.isEnabledByDefault() && gate.isEnabled()) {
                // It is disabled by default but it is enabled now => we need to disable it
                gateSettings.add("+" + gate.getName());
            }
        }

        return String.join(",", gateSettings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)  {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            FeatureGates other = (FeatureGates) o;
            return Objects.equals(continueOnManualRUFailure, other.continueOnManualRUFailure);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(continueOnManualRUFailure);
    }

    /**
     * Feature gate class represents individual feature fate
     */
    static class FeatureGate {
        private final String name;
        private final boolean defaultValue;
        private Boolean value = null;

        /**
         * Feature fate constructor
         *
         * @param name          Name of the feature gate
         * @param defaultValue  Default value of the feature gate
         */
        FeatureGate(String name, boolean defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        /**
         * @return  The name of the feature gate
         */
        public String getName() {
            return name;
        }

        /**
         * @return  Returns true if the value for this feature gate is already set or false if it is still null
         */
        public boolean isSet() {
            return value != null;
        }

        /**
         * Sets the value of the feature gate
         *
         * @param value Value of the feature gate
         */
        public void setValue(boolean value) {
            this.value = value;
        }

        /**
         * @return  True if the feature gate is enabled. False otherwise.
         */
        public boolean isEnabled() {
            return value == null ? defaultValue : value;
        }

        /**
         * @return  Returns True if this feature gate is enabled by default. False otherwise.
         */
        public boolean isEnabledByDefault() {
            return defaultValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)  {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                FeatureGate other = (FeatureGate) o;
                return defaultValue == other.defaultValue && Objects.equals(name, other.name) && Objects.equals(value, other.value);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, defaultValue, value);
        }
    }
}
