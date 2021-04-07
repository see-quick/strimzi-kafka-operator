package io.strimzi.systemtest.storage;

import io.strimzi.systemtest.Constants;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExtensionContextStorage {

    // test-name -> objet...
    Map<String, ExtensionContext.Namespace> map;
    ExtensionContext extensionContext;

    public ExtensionContextStorage(ExtensionContext extensionContext) {
        this.map = new HashMap<>();
        this.extensionContext = extensionContext;

        // creating unique storage for test case
        ExtensionContext.Namespace uniqueStorage = ExtensionContext.Namespace.GLOBAL;

        extensionContext.getStore(uniqueStorage).put(Constants.CLUSTER_NAME_KEY, "my-cluster-" + new Random().nextInt(Integer.MAX_VALUE));
        // TODO:...
        extensionContext.getStore(uniqueStorage).put(Constants.NAMESPACE_KEY, "my-namespace-" + new Random().nextInt(Integer.MAX_VALUE));

        map.put(extensionContext.getDisplayName(), uniqueStorage);
    }

    public Map<String, ExtensionContext.Namespace> getMap() {
        return map;
    }

    public Object getClusterName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.CLUSTER_NAME_KEY);
    }

    public Object getNamespaceName() {
        return extensionContext.getStore(map.get(extensionContext.getDisplayName())).get(Constants.NAMESPACE_KEY);
    }

    public ExtensionContext getExtensionContext() {
        return extensionContext;
    }
}
