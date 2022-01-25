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
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        systemUnderTest = new StrimziKafkaContainer()
            .withKafkaVersion("2.4.0")
            .waitForRunning();

        assertThrows(UnknownKafkaVersionException.class, () -> systemUnderTest.start());
    }
}
