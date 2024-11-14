/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StrimziProducerConsumerContainerKRaftIT extends AbstractIT {

    private static StrimziKafkaContainer strimziKafkaContainer;
    private static String bootstrapServers;

    @BeforeAll
    static void setup() {
        strimziKafkaContainer = new StrimziKafkaContainer()
            .withKraft()
            .waitForRunning();

        strimziKafkaContainer.start();
        bootstrapServers = strimziKafkaContainer.getInternalBootstrapServers();
    }

    @AfterAll
    static void teardown() {
        if (strimziKafkaContainer != null) {
            strimziKafkaContainer.stop();
        }
    }

    @Test
    void testProducerAndConsumerWithDefaultConfigs() {
        String topicName = "default-topic";
        String messageContent = "Test message for default configs\n";

        // Start Producer
        StrimziProducerContainer producerContainer = new StrimziProducerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withMessageContent(messageContent)
            .withSync()
            .withLogging();

        producerContainer.start();

        // Start Consumer
        StrimziConsumerContainer consumerContainer = new StrimziConsumerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withGroup("default-group")
            .withFromBeginning()
            .withLogging();

        consumerContainer.start();

        // Wait for the consumer to receive the message
        Utils.waitFor("Consumer receives message",
            () -> consumerContainer.getLogs().contains(messageContent.trim()));

        // Assert that the consumer received the message
        assertTrue(consumerContainer.getLogs().contains(messageContent.trim()));

        // Clean up
        producerContainer.stop();
        consumerContainer.stop();
    }

    @Test
    void testProducerWithCustomPropertiesAndConsumer() {
        String topicName = "custom-props-topic";
        String messageContent = "Message with custom producer properties\n";

        // Start Producer with custom properties
        StrimziProducerContainer producerContainer = new StrimziProducerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withMessageContent(messageContent)
            .withProducerProperty("acks", "all")
            .withProducerProperty("retries", "3")
            .withSync()
            .withLogging();

        producerContainer.start();

        // Start Consumer
        StrimziConsumerContainer consumerContainer = new StrimziConsumerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withGroup("custom-props-group")
            .withFromBeginning()
            .withLogging();

        consumerContainer.start();

        // Wait for the consumer to receive the message
        Utils.waitFor("Consumer receives message with custom properties", Duration.ofSeconds(1).toMillis(), Duration.ofSeconds(20).toMillis(),
            () -> consumerContainer.getLogs().contains(messageContent.trim()));

        // Assert that the consumer received the message
        assertTrue(consumerContainer.getLogs().contains(messageContent.trim()));

        // Clean up
        producerContainer.stop();
        consumerContainer.stop();
    }

    @Test
    void testConsumerWithInclude() {
        String topicName = "whitelist-topic";
        String messageContent = "Message for whitelist test\n";

        // Start Producer
        StrimziProducerContainer producerContainer = new StrimziProducerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withMessageContent(messageContent)
            .withSync()
            .withLogging();

        producerContainer.start();

        // Start Consumer with whitelist pattern
        StrimziConsumerContainer consumerContainer = new StrimziConsumerContainer()
            .withBootstrapServer(bootstrapServers)
            .withInclude("whitelist-.*")
            .withGroup("whitelist-group")
            .withFromBeginning()
            .withLogging();

        consumerContainer.start();

        // Wait for the consumer to receive the message
        Utils.waitFor("Consumer receives message using whitelist",
            () -> consumerContainer.getLogs().contains(messageContent.trim()));

        // Assert that the consumer received the message
        assertThat(consumerContainer.getLogs().contains(messageContent.trim()), is(true));

        // Clean up
        producerContainer.stop();
        consumerContainer.stop();
    }

    @Test
    void testProducerWithMessageFile() {
        String topicName = "message-file-topic";

        // Prepare the message file in test resources
        // Ensure that messages.txt is placed in src/test/resources/messages.txt
        MountableFile messageFile = MountableFile.forClasspathResource("messages.txt");

        // Start Producer with message file
        StrimziProducerContainer producerContainer = new StrimziProducerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withMessageFile(messageFile)
            .withSync()
            .withLogging();

        producerContainer.start();

        // Start Consumer
        StrimziConsumerContainer consumerContainer = new StrimziConsumerContainer()
            .withBootstrapServer(bootstrapServers)
            .withTopic(topicName)
            .withGroup("message-file-group")
            .withFromBeginning()
            .withLogging();

        consumerContainer.start();

        // Wait for the consumer to receive all messages
        Utils.waitFor("Consumer receives all messages from file", Duration.ofSeconds(1).toMillis(), Duration.ofSeconds(20).toMillis(),
            () -> {
                String logs = consumerContainer.getLogs();
                return logs.contains("Hello, this is Strimzi test container!")
                    && logs.contains("This is a test message.")
                    && logs.contains("Another message.");
            });

        // Clean up
        producerContainer.stop();
        consumerContainer.stop();
    }
}