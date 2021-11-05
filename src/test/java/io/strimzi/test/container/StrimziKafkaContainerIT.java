/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaContainerIT {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainerIT.class);

    private StrimziKafkaContainer systemUnderTest;

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testStartContainerWithEmptyConfiguration() throws IOException {
        assumeDocker();
        systemUnderTest = new StrimziKafkaContainer.StrimziKafkaContainerBuilder()
            .withBrokerId(1)
            .build();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://localhost:" + systemUnderTest.getMappedPort(9092)));
    }

    @Test
    void testStartContainerWithSomeConfiguration() throws IOException {
        assumeDocker();

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = new StrimziKafkaContainer.StrimziKafkaContainerBuilder()
            .withBrokerId(1)
            .withKafkaConfigurationMap(kafkaConfiguration)
            .build();

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, CoreMatchers.containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, CoreMatchers.containsString("log.index.interval.bytes = 2048"));

        systemUnderTest.stop();
    }
}
