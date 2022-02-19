/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class StrimziKafkaContainerIT extends AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaContainerIT.class);

    private StrimziKafkaContainer systemUnderTest;

    @ParameterizedTest(name = "testStartContainerWithEmptyConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithEmptyConfiguration(final String imageName) {
        assumeDocker();
        systemUnderTest = new StrimziKafkaContainer(imageName)
            .withBrokerId(1)
            .waitForRunning();
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();
    }

    @ParameterizedTest(name = "testStartContainerWithSomeConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSomeConfiguration(final String imageName) {
        assumeDocker();

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

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, CoreMatchers.containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.index.interval.bytes = 2048"));

        systemUnderTest.stop();
    }

    @ParameterizedTest(name = "testStartContainerWithFixedExposedPort-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithFixedExposedPort(final String imageName) {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer(imageName)
                .withPort(9092)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getMappedPort(9092), equalTo(9092));

        systemUnderTest.stop();
    }

    @ParameterizedTest(name = "testStartContainerWithSSLBootstrapServers-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSSLBootstrapServers(final String imageName) {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning()
                .withBootstrapServers(c -> String.format("SSL://%s:%s", c.getHost(), c.getMappedPort(9092)));
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("SSL://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();
    }

    @ParameterizedTest(name = "testStartContainerWithServerProperties-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithServerProperties(final String imageName) {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning()
                .withServerProperties(MountableFile.forClasspathResource("server.properties"));

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("auto.create.topics.enable = false"));

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();
    }

    @Test
    void testStartContainerWithStrimziKafkaImage() {
        assumeDocker();

        // explicitly set strimzi.test-container.kafka.custom.image
        String imageName = "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0";
        System.setProperty("strimzi.test-container.kafka.custom.image", imageName);

        systemUnderTest = new StrimziKafkaContainer()
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        assertThat(systemUnderTest.getDockerImageName(), is(imageName));
        systemUnderTest.stop();

        // empty
        System.setProperty("strimzi.test-container.kafka.custom.image", "");
    }

    @Test
    void testStartContainerWithCustomImage() {
        assumeDocker();

        String imageName = "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0";
        systemUnderTest = new StrimziKafkaContainer(imageName)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        assertThat(systemUnderTest.getDockerImageName(), is(imageName));
        systemUnderTest.stop();
    }

    @Test
    void testUnsupportedKafkaVersion() {
        assumeDocker();

        try {
            systemUnderTest = new StrimziKafkaContainer()
                .withKafkaVersion("2.4.0")
                .waitForRunning();

            assertThrows(UnknownKafkaVersionException.class, () -> systemUnderTest.start());
        } finally {
            systemUnderTest.stop();
        }
    }

    @ParameterizedTest(name = "testKafkaContainerConnectFromOutsideToInternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testKafkaContainerConnectFromOutsideToInternalZooKeeper() {
        assumeDocker();

        try {
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
        } finally {
            systemUnderTest.stop();
        }
    }

    @ParameterizedTest(name = "testKafkaContainerInternalCommunicationWithInternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testKafkaContainerInternalCommunicationWithInternalZooKeeper() throws IOException, InterruptedException {
        assumeDocker();

        try {
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
        } finally {
            systemUnderTest.stop();
        }
    }

    @ParameterizedTest(name = "testIllegalStateUsingInternalZooKeeperWithKraft-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testIllegalStateUsingInternalZooKeeperWithKraft() {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer()
            .withKraft()
            .waitForRunning();

        assertThrows(IllegalStateException.class, () -> systemUnderTest.getInternalZooKeeperConnect());
    }

    @ParameterizedTest(name = "testIllegalStateUsingInternalZooKeeperWithExternalZooKeeper-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testIllegalStateUsingInternalZooKeeperWithExternalZooKeeper() {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer()
            // we do not need to spin-up instance of StrimziZooKeeperContainer
            .withExternalZookeeperConnect("zookeeper:2181")
            .waitForRunning();

        assertThrows(IllegalStateException.class, () -> systemUnderTest.getInternalZooKeeperConnect());
    }
}
