/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziProducerContainerIT {

    @Test
    void testStartContainerWithEmptyConfiguration() {
        StrimziProducerContainer producerContainer = null;
        StrimziConsumerContainer consumerContainer = null;

        try (StrimziKafkaContainer systemUnderTest = new StrimziKafkaContainer()
            .withKraft()
            .withKafkaConfigurationMap(Map.of("auto.create.topics.enable", "true"))
            .waitForRunning()
        ) {

            systemUnderTest.start();

            assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://" + systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));

            producerContainer = new StrimziProducerContainer()
                .withBootstrapServer(systemUnderTest.getInternalBootstrapServers())
                .withTopic("my-topic")
                .withBatchSize(100)
                .withCompressionCodec("gzip")
                .withProducerProperty("acks", "all")
                .withSync()
                .withTimeout(5000)
                .withLogging()
                .withMessageContent("message1\n");

            producerContainer.start();

            consumerContainer = new StrimziConsumerContainer()
                .withKafkaVersion("3.9.0")
                .withBootstrapServer(systemUnderTest.getInternalBootstrapServers())
                .withGroup("test-group")
                .withTopic("my-topic")
                .withFromBeginning()
                .withLogging();

            consumerContainer.start();

            final StrimziConsumerContainer tempConsumerContainer = consumerContainer;
            Utils.waitFor("Consumer will receive messages", Duration.ofSeconds(1).toMillis(), Duration.ofSeconds(20).toMillis(),
                () -> {
                    String consumerLogs = tempConsumerContainer.getLogs();

                    return consumerLogs.contains("message1");
                });
        } finally {
            if (producerContainer != null) {
                producerContainer.stop();
            }
            if (consumerContainer != null) {
                consumerContainer.stop();
            }
        }
    }
}
