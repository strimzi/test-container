/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaContainerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaContainerIT.class);

    private StrimziKafkaContainer systemUnderTest;

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testStartContainerWithEmptyConfiguration() {
        assumeDocker();
        systemUnderTest = new StrimziKafkaContainer()
            .withBrokerId(1)
            .waitForRunning();
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();
    }

    @Test
    void testStartContainerWithSomeConfiguration() {
        assumeDocker();

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = new StrimziKafkaContainer()
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

    @Test
    void testStartContainerWithFixedExposedPort() {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer()
                .withPort(9092)
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getMappedPort(9092), equalTo(9092));

        systemUnderTest.stop();
    }

    @Test
    void testStartContainerWithSSLBootstrapServers() {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer()
                .waitForRunning()
                .withBootstrapServers(c -> String.format("SSL://%s:%s", c.getHost(), c.getMappedPort(9092)));
        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("SSL://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();
    }

    @Test
    void testStartContainerWithServerProperties() {
        assumeDocker();

        systemUnderTest = new StrimziKafkaContainer()
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

        // explicitly set strimzi.custom.image
        System.setProperty("strimzi.custom.image", "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0");

        systemUnderTest = new StrimziKafkaContainer()
                .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://"
                + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

        systemUnderTest.stop();

        // empty
        System.setProperty("strimzi.custom.image", "");
    }
}
