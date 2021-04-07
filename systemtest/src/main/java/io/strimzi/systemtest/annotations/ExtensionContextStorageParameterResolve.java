package io.strimzi.systemtest.annotations;

import io.strimzi.systemtest.storage.ExtensionContextStorage;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

public class ExtensionContextStorageParameterResolve {

    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContextStorage extensionContextStorage) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == ExtensionContextStorage.class;

    }

    public Object resolveParameter(ParameterContext parameterContext, ExtensionContextStorage extensionContextStorage) throws ParameterResolutionException {
        return extensionContextStorage;
    }
}
