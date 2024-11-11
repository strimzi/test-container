/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class StrimziKafkaContainerIT extends AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaContainerIT.class);

    private StrimziKafkaContainer systemUnderTest;

    @ParameterizedTest(name = "testStartContainerWithEmptyConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithEmptyConfiguration(final String imageName) {
        try (StrimziKafkaContainer systemUnderTest = new StrimziKafkaContainer(imageName)
                .withBrokerId(1)
                .waitForRunning()) {

            systemUnderTest.start();

            assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                    + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));
        }
    }

    @ParameterizedTest(name = "testStartContainerWithSomeConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSomeConfiguration(final String imageName) {
        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = new StrimziKafkaContainer(imageName)
            .withBrokerId(1)
            .withKafkaConfigurationMap(kafkaConfiguration)
            .waitForRunning();

        systemUnderTest.start();
        assertThat(systemUnderTest.getClusterId(), nullValue());

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, CoreMatchers.containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.index.interval.bytes = 2048"));
    }

    @ParameterizedTest(name = "testStartContainerWithFixedExposedPort-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithFixedExposedPort(final String imageName) {
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .withPort(9092)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getMappedPort(9092), equalTo(9092));
    }

    @ParameterizedTest(name = "testStartContainerWithSSLBootstrapServers-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSSLBootstrapServers(final String imageName) {
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning()
                .withBootstrapServers(c -> String.format("SSL://%s:%s", c.getHost(), c.getMappedPort(9092)));
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("SSL://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));
    }

    @ParameterizedTest(name = "testStartContainerWithServerProperties-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithServerProperties(final String imageName) {
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning()
                .withServerProperties(MountableFile.forClasspathResource("server.properties"));

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("auto.create.topics.enable = false"));

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));
    }

    @Test
    void testStartContainerWithStrimziKafkaImage() {
        // explicitly set strimzi.test-container.kafka.custom.image
        String imageName = "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0";
        System.setProperty("strimzi.test-container.kafka.custom.image", imageName);

        systemUnderTest = new StrimziKafkaContainer()
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        assertThat(systemUnderTest.getDockerImageName(), is(imageName));

        // empty
        System.setProperty("strimzi.test-container.kafka.custom.image", "");
    }

    @Test
    void testStartContainerWithCustomImage() {
        String imageName = "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0";
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        assertThat(systemUnderTest.getDockerImageName(), is(imageName));
    }

    @Test
    void testStartContainerWithCustomNetwork() {
        Network network = Network.newNetwork();

        systemUnderTest = new StrimziKafkaContainer()
                .withNetwork(network)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        assertThat(systemUnderTest.getNetwork().getId(), is(network.getId()));
    }

    @Test
    void testUnsupportedKafkaVersion() {
        systemUnderTest = new StrimziKafkaContainer()
            .withKafkaVersion("2.4.0")
            .waitForRunning();

        assertThrows(UnknownKafkaVersionException.class, () -> systemUnderTest.start());
    }

    @ParameterizedTest(name = "testKafkaContainerConnectFromOutsideToInternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testKafkaContainerConnectFromOutsideToInternalZooKeeper() {
        systemUnderTest = new StrimziKafkaContainer()
            .waitForRunning();
        systemUnderTest.start();

        // Creates a socket address from a hostname and a port number
        final String[] hostNameWithPort = systemUnderTest.getInternalZooKeeperConnect().split(":");

        SocketAddress socketAddress = new InetSocketAddress(hostNameWithPort[0], Integer.parseInt(hostNameWithPort[1]));

        try (Socket socket = new Socket()) {
            LOGGER.info("Hostname: {}, and port: {}", hostNameWithPort[0], hostNameWithPort[1]);
            socket.connect(socketAddress, 5000);
        } catch (SocketTimeoutException exception) {
            LOGGER.error("SocketTimeoutException " + hostNameWithPort[0] + ":" + hostNameWithPort[1] + ". " + exception.getMessage());
            fail();
        } catch (IOException exception) {
            LOGGER.error(
                "IOException - Unable to connect to " + hostNameWithPort[0] + ":" + hostNameWithPort[1] + ". " + exception.getMessage());
            fail();
        }
    }

    @ParameterizedTest(name = "testKafkaContainerInternalCommunicationWithInternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testKafkaContainerInternalCommunicationWithInternalZooKeeper() throws IOException, InterruptedException {
        systemUnderTest = new StrimziKafkaContainer()
            .waitForRunning();
        systemUnderTest.start();

        final Container.ExecResult result = this.systemUnderTest.execInContainer(
            "sh", "-c",
            "bin/zookeeper-shell.sh localhost:" + StrimziZookeeperContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
        );

        final String brokers = result.getStdout();

        assertThat(result.getExitCode(), is(0)); // 0 -> success
        assertThat(brokers, CoreMatchers.containsString("[0]"));
    }

    @ParameterizedTest(name = "testIllegalStateUsingInternalZooKeeperWithKraft-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testIllegalStateUsingInternalZooKeeperWithKraft() {
        systemUnderTest = new StrimziKafkaContainer()
            .withKraft()
            .waitForRunning();

        assertThrows(IllegalStateException.class, () -> systemUnderTest.getInternalZooKeeperConnect());
    }

    @ParameterizedTest(name = "testIllegalStateUsingInternalZooKeeperWithExternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testIllegalStateUsingInternalZooKeeperWithExternalZooKeeper() {
        systemUnderTest = new StrimziKafkaContainer()
            // we do not need to spin-up instance of StrimziZooKeeperContainer
            .withExternalZookeeperConnect("zookeeper:2181")
            .waitForRunning();

        assertThrows(IllegalStateException.class, () -> systemUnderTest.getInternalZooKeeperConnect());
    }

    @ParameterizedTest(name = "testStartBrokerWithProxyContainer-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartBrokerWithProxyContainer(final String imageName) {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"));

        systemUnderTest = new StrimziKafkaContainer(imageName)
                .withProxyContainer(proxyContainer)
                .waitForRunning();
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(),
                is(String.format("PLAINTEXT://%s", systemUnderTest.getProxy().getListen())));
    }

    @ParameterizedTest(name = "testGetProxyWithNoContainer-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testGetProxyWithNoContainer(final String imageName) {
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning();
        systemUnderTest.start();
        assertThrows(IllegalStateException.class, () -> systemUnderTest.getProxy());
        systemUnderTest.stop();
    }

    @Test
    void testWithKafkaLog() {
        systemUnderTest = new StrimziKafkaContainer()
            .waitForRunning()
            .withKafkaLog(Level.DEBUG);
        systemUnderTest.start();

        assertThat(systemUnderTest.getLogs(), CoreMatchers.containsString("INFO"));
        assertThat(systemUnderTest.getLogs(), CoreMatchers.containsString("DEBUG"));

        systemUnderTest.stop();
        systemUnderTest.withKafkaLog(Level.TRACE);
        systemUnderTest.start();

        assertThat(systemUnderTest.getLogs(), CoreMatchers.containsString("INFO"));
        assertThat(systemUnderTest.getLogs(), CoreMatchers.containsString("DEBUG"));
        assertThat(systemUnderTest.getLogs(), CoreMatchers.containsString("TRACE"));
    }

    @Test
    void testKafkaContainerFunctionality() {
        // using try-with-resources for AdminClient, KafkaProducer and KafkaConsumer (implicit closing connection)
        try (StrimziKafkaContainer systemUnderTest = new StrimziKafkaContainer()
                .waitForRunning()) {

            systemUnderTest.start();

            try (final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()));
                 KafkaProducer<String, String> producer = new KafkaProducer<>(
                         ImmutableMap.of(
                                 ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                                 ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                         ),
                         new StringSerializer(),
                         new StringSerializer()
                 );
                 KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                         ImmutableMap.of(
                                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                                 ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT)
                         ),
                         new StringDeserializer(),
                         new StringDeserializer())) {

                final String topicName = "example-topic";
                final String recordKey = "strimzi";
                final String recordValue = "the-best-project-in-the-world";

                final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

                consumer.subscribe(Collections.singletonList(topicName));

                producer.send(new ProducerRecord<>(topicName, recordKey, recordValue)).get();

                Utils.waitFor("Consumer records are present", Duration.ofSeconds(10).toMillis(), Duration.ofMinutes(2).toMillis(),
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        if (records.isEmpty()) {
                            return false;
                        }

                        // verify count
                        assertThat(records.count(), is(1));

                        ConsumerRecord<String, String> consumerRecord = records.records(topicName).iterator().next();

                        // verify content of the record
                        assertThat(consumerRecord.topic(), is(topicName));
                        assertThat(consumerRecord.key(), is(recordKey));
                        assertThat(consumerRecord.value(), is(recordValue));

                        return true;
                    });
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterEach
    void afterEach() {
        if (this.systemUnderTest != null) {
            this.systemUnderTest.stop();
        }
    }
}
