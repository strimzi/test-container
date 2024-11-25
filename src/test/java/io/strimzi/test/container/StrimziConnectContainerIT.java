/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziConnectContainerIT extends AbstractIT {

    private static final Set<String> MIRRORMAKER_CONNECTORS = Set.of(
            "MirrorSourceConnector",
            "MirrorCheckpointConnector",
            "MirrorHeartbeatConnector");

    private static final Set<String> FILE_CONNECTORS = Set.of(
            "FileStreamSinkConnector",
            "FileStreamSourceConnector");

    private StrimziConnectContainer systemUnderTest;
    private StrimziKafkaContainer kafka;

    @AfterEach
    void tearDown() {
        if (kafka != null) {
            kafka.stop();
        }
        if (systemUnderTest != null) {
            systemUnderTest.stop();
        }
    }

    @Test
    void testStartContainerWithoutBootstrapServers() {
        systemUnderTest = new StrimziConnectContainer();
        assertThrows(IllegalStateException.class, () -> systemUnderTest.start());
    }

    @Test
    void testStartContainer() throws Exception {
        kafka = new StrimziKafkaContainer()
                .withNetworkAliases("kafka")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();
        kafka.start();

        systemUnderTest = new StrimziConnectContainer()
                .withNetwork(kafka.getNetwork())
                .withBootstrapServers("kafka:9091")
                .waitForRunning();
        systemUnderTest.start();

        String info = query(systemUnderTest, "/");
        assertThat(info, containsString(kafka.getClusterId()));

        String connectors = query(systemUnderTest, "/connector-plugins");
        for (String connector : MIRRORMAKER_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
        for (String connector : FILE_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
    }

    @Test
    void testStartContainerWithoutFileConnectors() throws Exception {
        kafka = new StrimziKafkaContainer()
                .withNetworkAliases("kafka")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();
        kafka.start();

        systemUnderTest = new StrimziConnectContainer()
                .withNetwork(kafka.getNetwork())
                .withBootstrapServers("kafka:9091")
                .withIncludeFileConnectors(false)
                .waitForRunning();
        systemUnderTest.start();

        String info = query(systemUnderTest, "/");
        assertThat(info, containsString(kafka.getClusterId()));

        String connectors = query(systemUnderTest, "/connector-plugins");
        for (String connector : MIRRORMAKER_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
        for (String connector : FILE_CONNECTORS) {
            assertThat(connectors, not(containsString(connector)));
        }
    }

    @Test
    void testStartContainerWithDebugLogs() {
        kafka = new StrimziKafkaContainer()
                .withNetworkAliases("kafka")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();
        kafka.start();

        systemUnderTest = new StrimziConnectContainer()
                .withNetwork(kafka.getNetwork())
                .withBootstrapServers("kafka:9091")
                .withConnectLog(Level.DEBUG)
                .waitForRunning();
        systemUnderTest.start();

        assertThat(systemUnderTest.getLogs(), containsString("] DEBUG "));
    }

    @Test
    void testStartContainerWithConfigurationMap() throws Exception {
        kafka = new StrimziKafkaContainer()
                .withNetworkAliases("kafka")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();
        kafka.start();

        systemUnderTest = new StrimziConnectContainer()
                .withNetwork(kafka.getNetwork())
                .withBootstrapServers("kafka:9091")
                .withConnectConfigurationMap(Map.of("plugin.path", "/tmp"))
                .waitForRunning();
        systemUnderTest.start();

        String connectors = query(systemUnderTest, "/connector-plugins");
        for (String connector : FILE_CONNECTORS) {
            assertThat(connectors, not(containsString(connector)));
        }
    }

    private String query(StrimziConnectContainer container, String path) throws Exception {
        HttpClient httpClient = HttpClient.newHttpClient();
        URI uri = new URI(container.restEndpoint() + path);
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpURLConnection.HTTP_OK));
        return response.body();
    }
}
