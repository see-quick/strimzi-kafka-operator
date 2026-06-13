/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the project root for details).
 */
package io.strimzi.systemtest.performance.regression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResultMetadata {

    private final String commitSha;
    private final String branch;
    private final String timestamp;
    private final String kubernetesVersion;
    private final String kafkaVersion;
    private final String strimziVersion;

    @JsonCreator
    public ResultMetadata(
        @JsonProperty("commitSha") String commitSha,
        @JsonProperty("branch") String branch,
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("kubernetesVersion") String kubernetesVersion,
        @JsonProperty("kafkaVersion") String kafkaVersion,
        @JsonProperty("strimziVersion") String strimziVersion
    ) {
        this.commitSha = commitSha;
        this.branch = branch;
        this.timestamp = timestamp;
        this.kubernetesVersion = kubernetesVersion;
        this.kafkaVersion = kafkaVersion;
        this.strimziVersion = strimziVersion;
    }

    public String getCommitSha() {
        return commitSha;
    }

    public String getBranch() {
        return branch;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getKubernetesVersion() {
        return kubernetesVersion;
    }

    public String getKafkaVersion() {
        return kafkaVersion;
    }

    public String getStrimziVersion() {
        return strimziVersion;
    }
}
