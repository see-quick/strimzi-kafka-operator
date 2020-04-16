/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectUtils.class);

    private KafkaConnectUtils() {}

    public static void createFileSinkConnector(String podName, String topicName, String sinkFileName, String apiUrl) {
        cmdKubeClient().execInPod(podName, "/bin/bash", "-c",
            "curl -X POST -H \"Content-Type: application/json\" " + "--data '{ \"name\": \"sink-test\", " +
                "\"config\": " + "{ \"connector.class\": \"FileStreamSink\", " +
                "\"tasks.max\": \"1\", \"topics\": \"" + topicName + "\"," + " \"file\": \"" + sinkFileName + "\" } }' " +
                    apiUrl + "/connectors"
        );
    }

    public static void waitForConnectStatus(String name, String status) {
        LOGGER.info("Waiting for Kafka Connect {} state: {}", name, status);
        TestUtils.waitFor("Kafka Connect " + name + " state: " + status, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> KafkaConnectResource.kafkaConnectClient().inNamespace(kubeClient().getNamespace()).withName(name).get().getStatus().getConditions().get(0).getType().equals(status));
        LOGGER.info("Kafka Connect {} is in desired state: {}", name, status);
    }

    public static void waitUntilKafkaConnectRestApiIsAvailable(String podNamePrefix) {
        LOGGER.info("Waiting until kafka connect service is present");
        TestUtils.waitFor("Waiting until kafka connect service is present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> cmdKubeClient().execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("Kafka connect service is present");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName, String message) {
        LOGGER.info("Waiting for messages in file sink");
        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(message));
        LOGGER.info("Expected messages are in file sink");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName) {
        waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, sinkFileName,
                "\"Sending messages\": \"Hello-world - 99\"");
    }

    /**
     *  Method waitForKafkaConnectConfigChange, which will wait until the kafka connect CR config will be changed
     * @param propertyKey property key in the Kafka Connect CR config
     * @param propertyValue property value in the Kafka Connect CR config
     * @param namespace namespace name
     * @param clusterName cluster name
     */
    public static void waitForKafkaConnectConfigChange(String propertyKey, String propertyValue, String namespace, String clusterName) {
        LOGGER.info("Waiting for Kafka Connect property {} -> {} change", propertyKey, propertyValue);
        TestUtils.waitFor("Waiting for Kafka Connect config " + propertyKey + " -> " + propertyValue, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                LOGGER.debug("Property key -> {}, Current property value -> {}", propertyKey, KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getSpec().getConfig().get(propertyKey));
                LOGGER.debug(KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getSpec().getConfig().get(propertyKey) + " == " + propertyValue);
                return KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getSpec().getConfig().get(propertyKey).equals(propertyValue);
            });
        LOGGER.info("Kafka Connect property {} -> {} change", propertyKey, propertyValue);
    }
}
