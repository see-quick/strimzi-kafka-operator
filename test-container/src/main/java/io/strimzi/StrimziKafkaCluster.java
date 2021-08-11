/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StrimziKafkaCluster implements Startable {

    private static final Logger LOGGER = LogManager.getLogger(StrimziZookeeperContainer.class);

    private final int brokersNum;
    private final Network network;
    private final GenericContainer<?> zookeeper;
    private final Collection<StrimziKafkaContainer> brokers;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public StrimziKafkaCluster(String version, int brokersNum, int internalTopicReplicationFactor, Map<String, String> additionalKafkaConfiguration) {
        if (brokersNum < 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
        if (internalTopicReplicationFactor < 0 || internalTopicReplicationFactor > brokersNum) {
            throw new IllegalArgumentException("internalTopicReplicationFactor '" + internalTopicReplicationFactor + "' must be less than brokersNum and greater than 0");
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        this.zookeeper = new StrimziZookeeperContainer()
            .withNetwork(this.network)
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(StrimziZookeeperContainer.ZOOKEEPER_PORT));

        Map<String, String> defaultKafkaConfigurationForMultiNode = new HashMap<>();
        defaultKafkaConfigurationForMultiNode.put("offsets.topic.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("num.partitions", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.replication.factor", String.valueOf(internalTopicReplicationFactor));
        defaultKafkaConfigurationForMultiNode.put("transaction.state.log.min.isr", String.valueOf(internalTopicReplicationFactor));

        additionalKafkaConfiguration.putAll(defaultKafkaConfigurationForMultiNode);

        // multi-node set up
        this.brokers = IntStream
            .range(0, this.brokersNum)
            .mapToObj(brokerId -> {
                LOGGER.info("Starting broker with id {}", brokerId);
                // adding broker id for each kafka container
                additionalKafkaConfiguration.put("broker.id", String.valueOf(brokerId));

                return new StrimziKafkaContainer(version, additionalKafkaConfiguration)
                    .withNetwork(this.network)
                    .withNetworkAliases("broker-" + brokerId)
                    .dependsOn(this.zookeeper)
                    .withExternalZookeeper("zookeeper:" + StrimziZookeeperContainer.ZOOKEEPER_PORT);
            })
            .collect(Collectors.toList());
    }

    public Collection<StrimziKafkaContainer> getBrokers() {
        return this.brokers;
    }

    public String getBootstrapServers() {
        return brokers.stream()
            .map(StrimziKafkaContainer::getBootstrapServers)
            .collect(Collectors.joining(","));
    }

    @Override
    public void start() {
        Stream<Startable> startables = this.brokers.stream().map(Startable.class::cast);
        try {
            Startables.deepStart(startables).get(60, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }

        TestUtils.waitFor("Zookeeper readiness", Duration.ofSeconds(1).toMillis(), Duration.ofSeconds(30).toMillis(),
            () -> {
                Container.ExecResult result = null;
                try {
                    result = this.zookeeper.execInContainer(
                        "sh", "-c",
                        "bin/zookeeper-shell.sh zookeeper:" + StrimziKafkaContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
                    );
                    String brokers = result.getStdout();

                    LOGGER.info("Stdout from zookeeper container....{}", result.getStdout());

                    return brokers != null && brokers.split(",").length == this.brokersNum;
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
            });
    }

    @Override
    public void stop() {
        // stop all containers in parallel
        Stream.concat(this.brokers.stream(), Stream.of(this.zookeeper))
            .parallel()
            .forEach(GenericContainer::stop);
    }

    public GenericContainer<?> getZookeeper() {
        return zookeeper;
    }
}