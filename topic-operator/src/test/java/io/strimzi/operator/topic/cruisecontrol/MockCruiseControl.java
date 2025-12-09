/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.strimzi.operator.common.CruiseControlUtil;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.operator.topic.TestUtil;
import io.strimzi.test.ReadWriteUtils;

import java.io.File;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

/**
 * Cruise Control mock using WireMock.
 */
public class MockCruiseControl {
    private final WireMockServer server;

    /**
     * Sets up and returns a Cruise Control mock server.
     *
     * @param serverPort   The port number the server should listen on.
     * @param tlsKeyFile   File containing the CA key.
     * @param tlsCrtFile   File containing the CA crt.
     */
    public MockCruiseControl(int serverPort, File tlsKeyFile, File tlsCrtFile) {
        try {
            WireMockConfiguration config = WireMockConfiguration.options()
                    .port(serverPort)
                    .httpsPort(serverPort + 1)
                    .keystorePath(tlsKeyFile.getAbsolutePath())
                    .keystorePassword("changeit")
                    .keyManagerPassword("changeit");

            this.server = new WireMockServer(config);
            this.server.start();
            WireMock.configureFor("localhost", serverPort);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        server.resetAll();
    }

    public void stop() {
        server.stop();
    }

    public boolean isRunning() {
        return server.isRunning();
    }

    public void expectTopicConfigSuccessResponse(File apiUserFile, File apiPassFile) {
        String successJson = ReadWriteUtils.readFileFromResources(getClass(), "/cruisecontrol/topic-config-success.json");
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        // encryption and authentication disabled
        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h")
                        .withBody(successJson)));

        // encryption and authentication enabled
        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h")
                        .withBody(successJson)));

        // encryption only
        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h")
                        .withBody(successJson)));

        // authentication only
        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h")
                        .withBody(successJson)));
    }

    public void expectTopicConfigErrorResponse(File apiUserFile, File apiPassFile) {
        String failureJson = ReadWriteUtils.readFileFromResources(getClass(), "/cruisecontrol/topic-config-failure.json");
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withHeader("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h")
                        .withBody(failureJson)));
    }

    public void expectTopicConfigRequestTimeout(File apiUserFile, File apiPassFile) {
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(408)));
    }

    public void expectTopicConfigRequestUnauthorized(File apiUserFile, File apiPassFile) {
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(post(urlPathEqualTo(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString()))
                .withQueryParam(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), matching("true|false"))
                .withQueryParam(CruiseControlParameters.DRY_RUN.toString(), equalTo("false"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(401)));
    }

    public void expectUserTasksSuccessResponse(File apiUserFile, File apiPassFile) {
        String successJson = ReadWriteUtils.readFileFromResources(getClass(), "/cruisecontrol/user-tasks-success.json");
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        // encryption and authentication disabled
        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(successJson)));

        // encryption and authentication enabled
        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(successJson)));

        // encryption only
        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(successJson)));

        // authentication only
        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(successJson)));
    }

    public void expectUserTasksErrorResponse(File apiUserFile, File apiPassFile) {
        String failureJson = ReadWriteUtils.readFileFromResources(getClass(), "/cruisecontrol/user-tasks-failure.json");
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody(failureJson)));
    }

    public void expectUserTasksRequestTimeout(File apiUserFile, File apiPassFile) {
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(408)));
    }

    public void expectUserTasksRequestUnauthorized(File apiUserFile, File apiPassFile) {
        String authHeaderValue = CruiseControlUtil.buildBasicAuthValue(
                TestUtil.contentFromTextFile(apiUserFile), TestUtil.contentFromTextFile(apiPassFile));

        server.stubFor(get(urlPathEqualTo(CruiseControlEndpoints.USER_TASKS.toString()))
                .withQueryParam(CruiseControlParameters.USER_TASK_IDS.toString(), equalTo("8911ca89-351f-888-8d0f-9aade00e098h"))
                .withQueryParam(CruiseControlParameters.JSON.toString(), equalTo("true"))
                .withHeader("Authorization", equalTo(authHeaderValue))
                .willReturn(aResponse()
                        .withStatus(401)));
    }
}