/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.collectors;

import io.skodjob.kubetest4j.MetricsCollector;
import io.skodjob.kubetest4j.MetricsComponent;

import java.util.List;
import java.util.regex.Pattern;

public class ClusterOperatorMetricsCollector extends BaseMetricsCollector {

    public ClusterOperatorMetricsCollector(MetricsCollector.Builder builder) {
        super(builder);
    }

    public List<Double> getReconciliationsTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsSuccessfulTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_successful_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsFailedTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_failed_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsDurationSecondsSum(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_duration_seconds_sum\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsDurationSecondsMax(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_duration_seconds_max\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getReconciliationsAlreadyEnqueuedTotal(String kind) {
        Pattern pattern = Pattern.compile("strimzi_reconciliations_already_enqueued_total\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    public List<Double> getResourceCount(String kind) {
        Pattern pattern = Pattern.compile("strimzi_resources\\{kind=\"" + kind + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    @Override
    protected ClusterOperatorMetricsCollector.Builder newBuilder() {
        return new ClusterOperatorMetricsCollector.Builder();
    }

    @Override
    protected ClusterOperatorMetricsCollector.Builder updateBuilder(BaseMetricsCollector.Builder builder) {
        return (ClusterOperatorMetricsCollector.Builder) super.updateBuilder(builder);
    }

    @Override
    public ClusterOperatorMetricsCollector.Builder toBuilder() {
        return this.updateBuilder(this.newBuilder());
    }

    public static class Builder extends BaseMetricsCollector.Builder {
        @Override
        public ClusterOperatorMetricsCollector build() {
            return new ClusterOperatorMetricsCollector(this);
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withNamespaceName(String namespaceName) {
            super.withNamespaceName(namespaceName);
            return this;
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withScraperPodName(String scraperPodName) {
            super.withScraperPodName(scraperPodName);
            return this;
        }

        @Override
        public ClusterOperatorMetricsCollector.Builder withComponent(MetricsComponent component) {
            super.withComponent(component);
            return this;
        }
    }
}
