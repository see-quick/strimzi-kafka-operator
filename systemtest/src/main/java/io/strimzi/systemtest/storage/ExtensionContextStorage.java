package io.strimzi.systemtest.storage;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExtensionContextStorage {

    // test-name -> objet...
    Map<String, ExtensionContext.Namespace> map;
    ExtensionContext extensionContext;
    ExtensionContext.Namespace uniqueStorage;

    public ExtensionContextStorage(ExtensionContext extensionContext) {
        this.map = new HashMap<>();
        this.extensionContext = extensionContext;

        // creating unique storage for test case
        uniqueStorage = ExtensionContext.Namespace.GLOBAL;

        // cluster data
        String clusterName = "my-cluster-" + new Random().nextInt(Integer.MAX_VALUE);

        extensionContext.getStore(uniqueStorage).put(Constants.CLUSTER_NAME_KEY, clusterName);
        extensionContext.getStore(uniqueStorage).put(Constants.SOURCE_CLUSTER_NAME_KEY, clusterName + "-source");
        extensionContext.getStore(uniqueStorage).put(Constants.TARGET_CLUSTER_NAME_KEY, clusterName + "-target");

        // topic data
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        extensionContext.getStore(uniqueStorage).put(Constants.TOPIC_NAME_KEY, topicName);
        extensionContext.getStore(uniqueStorage).put(Constants.SOURCE_TOPIC_NAME_KEY, "availability-topic-source-" + topicName);
        extensionContext.getStore(uniqueStorage).put(Constants.TARGET_TOPIC_NAME_KEY, "availability-topic-target-" + topicName);

        // kafka user data
        extensionContext.getStore(uniqueStorage).put(Constants.KAFKA_USER_NAME_KEY, KafkaUserUtils.generateRandomNameOfKafkaUser());

        // kafka clients data
        extensionContext.getStore(uniqueStorage).put(Constants.KAFKA_CLIENTS_NAME_KEY, clusterName + "-" + Constants.KAFKA_CLIENTS);

        // namespace data
        extensionContext.getStore(uniqueStorage).put(Constants.NAMESPACE_KEY, "my-namespace-" + new Random().nextInt(Integer.MAX_VALUE));

        map.put(extensionContext.getDisplayName(), uniqueStorage);
    }

    public Map<String, ExtensionContext.Namespace> getMap() {
        return map;
    }

    public String getClusterName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.CLUSTER_NAME_KEY).toString();
    }

    public String getSourceClusterName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.SOURCE_CLUSTER_NAME_KEY).toString();
    }

    public String getTargetClusterName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.TARGET_CLUSTER_NAME_KEY).toString();
    }

    public String getTopicName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.TOPIC_NAME_KEY).toString();
    }

    public String getSourceTopicName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.SOURCE_TOPIC_NAME_KEY).toString();
    }

    public String getTargetTopicName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.TARGET_TOPIC_NAME_KEY).toString();
    }

    public String getKafkaUserName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.KAFKA_USER_NAME_KEY).toString();
    }

    public String getKafkaClientsName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.KAFKA_CLIENTS_NAME_KEY).toString();
    }

    public String getNamespaceName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.NAMESPACE_KEY).toString();
    }


    public ExtensionContext getExtensionContext() {
        return extensionContext;
    }

    public ExtensionContext.Namespace getUniqueStorage() {
        return uniqueStorage;
    }
}
