/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziConnectClusterIT {

    public static final Set<String> MIRROR_MAKER_CONNECTORS = Set.of(
            "MirrorSourceConnector",
            "MirrorCheckpointConnector",
            "MirrorHeartbeatConnector");

    public static final Set<String> FILE_CONNECTORS = Set.of(
            "FileStreamSinkConnector",
            "FileStreamSourceConnector");

    private StrimziKafkaCluster kafkaCluster;
    private StrimziConnectCluster connectCluster;

    @BeforeEach
    public void setUp() {
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withSharedNetwork()
                .withNumberOfBrokers(1)
                .build();
        kafkaCluster.start();
    }

    @AfterEach
    public void tearDown() {
        kafkaCluster.stop();
        connectCluster.stop();
    }

    @Test
    public void testBasicCluster() throws Exception {
        connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withGroupId("my-cluster")
                .withKafkaCluster(kafkaCluster)
                .build();
        connectCluster.start();

        assertThat(connectCluster.getWorkers().size(), is(1));
        String root = httpGet("/");
        assertThat(root, containsString(getClusterId()));
        String connectors = httpGet("/connector-plugins");
        for (String connector : MIRROR_MAKER_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
        for (String connector : FILE_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
    }

    @Test
    public void testDisableFileConnectors() throws Exception {
        connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withGroupId("my-cluster")
                .withKafkaCluster(kafkaCluster)
                .withoutFileConnectors()
                .build();
        connectCluster.start();

        assertThat(connectCluster.getWorkers().size(), is(1));
        String root = httpGet("/");
        assertThat(root, containsString(getClusterId()));
        String connectors = httpGet("/connector-plugins");
        for (String connector : MIRROR_MAKER_CONNECTORS) {
            assertThat(connectors, containsString(connector));
        }
        for (String connector : FILE_CONNECTORS) {
            assertThat(connectors, not(containsString(connector)));
        }
    }

    @Test
    public void testKafkaVersion() throws Exception {
        String version = "3.9.0";
        connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withGroupId("my-cluster")
                .withKafkaCluster(kafkaCluster)
                .withKafkaVersion(version)
                .build();
        connectCluster.start();

        assertThat(connectCluster.getWorkers().size(), is(1));
        String root = httpGet("/");
        assertThat(root, containsString(getClusterId()));
        assertThat(root, containsString(version));
    }

    @Test
    public void testOverrideConfigs() throws Exception {
        String offsetTopic = "custom-offset-topic";
        connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withGroupId("my-cluster")
                .withKafkaCluster(kafkaCluster)
                .withAdditionalConnectConfiguration(Map.of("offset.storage.topic", offsetTopic))
                .build();
        connectCluster.start();

        assertThat(connectCluster.getWorkers().size(), is(1));
        String root = httpGet("/");
        assertThat(root, containsString(getClusterId()));
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {
            Set<String> topics = admin.listTopics().names().get();
            assertThat(topics, hasItem(offsetTopic));
        }
    }

    @Test
    public void testRunConnector() throws Exception {
        String topic = "topic-to-export";
        String file = "/tmp/sink.out";
        String connectorName = "file-sink";
        List<String> records = new ArrayList<>();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) -1)));
        }
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ))) {
            for (int i = 0; i < 5; i++) {
                String value = "record" + i;
                producer.send(new ProducerRecord<>(topic, value));
                records.add(value);
            }
        }
        connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
                .withGroupId("my-cluster")
                .withKafkaCluster(kafkaCluster)
                .build();
        connectCluster.start();
        assertThat(connectCluster.getWorkers().size(), is(1));
        String root = httpGet("/");
        assertThat(root, containsString(getClusterId()));

        String connectorConfig =
                "{\n" +
                "  \"connector.class\":\"org.apache.kafka.connect.file.FileStreamSinkConnector\",\n" +
                "  \"topics\": \"" + topic + "\",\n" +
                "  \"file\": \"" + file + "\"\n" +
                "}";
        String config = httpPut("/connectors/" + connectorName + "/config", connectorConfig);
        assertThat(config, containsString("\"name\":\"" + connectorName + "\""));
        assertThat(config, containsString("\"type\":\"sink\""));
        String connectors = httpGet("/connectors");
        assertThat(connectors, containsString(connectorName));

        GenericContainer<?> worker = connectCluster.getWorkers().iterator().next();

        for (String record : records) {
            Utils.waitFor("Checking " + record + " is in " + file,
                Duration.ofSeconds(5),
                Duration.ofMinutes(1),
                () -> {
                    try {
                        Container.ExecResult result = worker.execInContainer("sh", "-c", "cat " + file);
                        return result.getStdout().contains(record);
                    } catch (Exception exc) {
                        return false;
                    }
                }
            );
        }
    }

    public String httpGet(String path) throws Exception {
        HttpClient httpClient = HttpClient.newHttpClient();
        URI uri = new URI(connectCluster.getRestEndpoint() + path);
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpURLConnection.HTTP_OK));
        return response.body();
    }

    public String httpPut(String path, String body) throws Exception {
        HttpClient httpClient = HttpClient.newHttpClient();
        URI uri = new URI(connectCluster.getRestEndpoint() + path);
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .setHeader("Content-Type", "application/json")
                .uri(uri)
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(HttpURLConnection.HTTP_CREATED));
        return response.body();
    }

    String getClusterId() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {
            return admin.describeCluster().clusterId().get();
        }
    }
}
