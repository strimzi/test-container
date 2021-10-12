/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaContainerTest {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainerTest.class);

    private StrimziKafkaContainer systemUnderTest;

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testStartContainerWithEmptyConfiguration() {
        assumeDocker();
        systemUnderTest = StrimziKafkaContainer.create(1);

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://localhost:" + systemUnderTest.getMappedPort(9092)));
    }

    @Test
    void testStartContainerWithSomeConfiguration() {
        assumeDocker();

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = StrimziKafkaContainer.createWithAdditionalConfiguration(1, kafkaConfiguration);

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

        systemUnderTest.stop();
    }
}
