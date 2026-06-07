/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import io.strimzi.systemtest.performance.PerformanceConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ClusterOperatorMetricsParser extends BasePerformanceMetricsParser {

    private static final Set<String> METRICS_OF_INTEREST = Set.of(
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX),
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_TOTAL),
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_SUCCESSFUL_TOTAL),
        getMetricFileName(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT),
        getMetricFileName(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL)
    );

    public ClusterOperatorMetricsParser() {
        super();
    }

    @Override
    protected String[] extractAndFormatRowData(int experimentNumber, ExperimentMetrics experimentMetrics) {
        final List<String> rowData = new ArrayList<>();

        rowData.add(String.valueOf(experimentNumber));

        for (final Map.Entry<String, String> testMetric : experimentMetrics.getTestMetrics().entrySet()) {
            rowData.add(testMetric.getValue());
        }

        for (final Map.Entry<String, List<Double>> componentMetric : experimentMetrics.getComponentMetrics().entrySet()) {
            if (METRICS_OF_INTEREST.contains(componentMetric.getKey())) {
                rowData.add(String.valueOf(getMaxValueFromList(componentMetric.getValue())));
            }
        }

        return rowData.toArray(new String[0]);
    }

    @Override
    protected String[] getHeadersForUseCase(ExperimentMetrics experimentMetrics) {
        final List<String> headers = new ArrayList<>();

        headers.add("Experiment");

        for (final Map.Entry<String, String> testMetric : experimentMetrics.getTestMetrics().entrySet()) {
            headers.add(testMetric.getKey());
        }

        for (final Map.Entry<String, List<Double>> componentMetric : experimentMetrics.getComponentMetrics().entrySet()) {
            if (METRICS_OF_INTEREST.contains(componentMetric.getKey())) {
                headers.add(componentMetric.getKey());
            }
        }

        return headers.toArray(new String[0]);
    }

    @Override
    public void parseMetrics() throws IOException {
        this.parseLatestMetrics(PerformanceConstants.CLUSTER_OPERATOR_PARSER);
    }

    @Override
    protected void showMetrics() {
        System.out.println(this.buildResultTable());
    }

    @Override
    protected String getSortKey() {
        return null;
    }
}
