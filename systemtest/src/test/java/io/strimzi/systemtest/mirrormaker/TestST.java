package io.strimzi.systemtest.mirrormaker;

import io.strimzi.systemtest.storage.ExtensionContextStorage;
import io.strimzi.test.interfaces.ExtensionContextParameterResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(ExtensionContextParameterResolver.class)
public class TestST {

    Map<String, ExtensionContextStorage> mapOfStorages = new HashMap<>();

    @Test
    void test1(ExtensionContext extensionContext) {
        ExtensionContextStorage storage = mapOfStorages.get(extensionContext.getDisplayName());

        System.out.println(storage.getClusterName());
        System.out.println(storage.getNamespaceName());
    }

    @Test
    void test2(ExtensionContext extensionContext) {
        ExtensionContextStorage storage = mapOfStorages.get(extensionContext.getDisplayName());

        System.out.println(storage.getClusterName());
        System.out.println(storage.getNamespaceName());
    }

    @BeforeEach
    void beforeEach(ExtensionContext extensionContext) {
        mapOfStorages.put(extensionContext.getDisplayName(), new ExtensionContextStorage(extensionContext));
    }
}
