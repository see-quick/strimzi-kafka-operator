/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.storage;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;

/**
 * TestStorage generate and stores values in the specific @see{ExtensionContext}. This ensures that if one want to
 * retrieve data from TestStorage it can be done via ExtensionContext (with help of @ConcurrentHashMap)
 */
final public class TestStorage {

    private static final String PRODUCER = "hello-world-producer";
    private static final String CONSUMER = "hello-world-consumer";
    private static final String CLUSTER_NAME_PREFIX = "my-cluster-";

    private ExtensionContext extensionContext;
    private String namespaceName;
    private String clusterName;
    private String topicName;
    private String streamsTopicTargetName;
    private String kafkaClientsName;
    private String producerName;
    private String consumerName;

    public TestStorage(ExtensionContext extensionContext) {
        this.extensionContext = extensionContext;
        this.namespaceName = StUtils.getNamespaceBasedOnRbac(INFRA_NAMESPACE, extensionContext);
        this.clusterName = CLUSTER_NAME_PREFIX + new Random().nextInt(Integer.MAX_VALUE);
        this.topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.streamsTopicTargetName = KafkaTopicUtils.generateRandomNameOfTopic();
        this.kafkaClientsName = clusterName + "-" + Constants.KAFKA_CLIENTS;
        this.producerName = clusterName + "-" + PRODUCER;
        this.consumerName = clusterName  + "-" + CONSUMER;

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, namespaceName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CLUSTER_KEY, clusterName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.TOPIC_KEY, topicName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.STREAM_TOPIC_KEY, streamsTopicTargetName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.KAFKA_CLIENTS_KEY, kafkaClientsName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PRODUCER_KEY, producerName);
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.CONSUMER_KEY, consumerName);
    }

    public void addToTestStorage(String key, Object value) {
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(key, value);
    }

    public Object retrieveFromTestStorage(String key) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(key);
    }

    public String getNamespaceName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();
    }

    public String getClusterName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CLUSTER_KEY).toString();
    }
    public String getTopicName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.TOPIC_KEY).toString();
    }

    public String getKafkaClientsName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.KAFKA_CLIENTS_KEY).toString();
    }
    public String getProducerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PRODUCER_KEY).toString();
    }
    public String getConsumerName() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.CONSUMER_KEY).toString();
    }
}
