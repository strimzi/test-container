/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StrimziConsumerContainerTest {

    @Test
    public void testWithBootstrapServer() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
               .withBootstrapServer("localhost:9092");
        assertTrue(container.getCommandOptions().contains("--bootstrap-server localhost:9092"));
    }

    @Test
    public void testWithConsumerProperty() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withConsumerProperty("auto.offset.reset", "earliest");
        assertTrue(container.getCommandOptions().contains("--consumer-property auto.offset.reset=earliest"));
    }

    @Test
    public void testWithConsumerConfig() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withConsumerConfig(MountableFile.forClasspathResource("consumer.properties"));
        assertTrue(container.getCommandOptions().contains("--consumer.config /tmp/consumer.properties"));
    }

    @Test
    public void testWithEnableSystestEvents() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withEnableSystestEvents();
        assertTrue(container.getCommandOptions().contains("--enable-systest-events"));
    }

    @Test
    public void testWithFormatter() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
            .withFormatter("CustomFormatter");
        assertTrue(container.getCommandOptions().contains("--formatter CustomFormatter"));
    }

    @Test
    public void testWithFormatterConfig() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withFormatterConfig(MountableFile.forClasspathResource("formatter.properties"));
        assertTrue(container.getCommandOptions().contains("--formatter-config /tmp/formatter.properties"));
    }

    @Test
    public void testWithFromBeginning() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withFromBeginning();
        assertTrue(container.getCommandOptions().contains("--from-beginning"));
    }

    @Test
    public void testWithGroup() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withGroup("test-group");
        assertTrue(container.getCommandOptions().contains("--group test-group"));
    }

    @Test
    public void testWithInclude() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withInclude(".*test.*");
        assertTrue(container.getCommandOptions().contains("--include .*test.*"));
    }

    @Test
    public void testWithIsolationLevel() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withIsolationLevel("read_committed");
        assertTrue(container.getCommandOptions().contains("--isolation-level read_committed"));
    }

    @Test
    public void testWithKeyDeserializer() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        assertTrue(container.getCommandOptions().contains("--key-deserializer org.apache.kafka.common.serialization.StringDeserializer"));
    }

    @Test
    public void testWithMaxMessages() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withMaxMessages(100);
        assertTrue(container.getCommandOptions().contains("--max-messages 100"));
    }

    @Test
    public void testWithOffset() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withOffset("earliest");
        assertTrue(container.getCommandOptions().contains("--offset earliest"));
    }

    @Test
    public void testWithPartition() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withPartition(0);
        assertTrue(container.getCommandOptions().contains("--partition 0"));
    }

    @Test
    public void testWithProperty() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withProperty("print.timestamp", "true");
        assertTrue(container.getCommandOptions().contains("--property print.timestamp=true"));
    }

    @Test
    public void testWithSkipMessageOnError() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withSkipMessageOnError();
        assertTrue(container.getCommandOptions().contains("--skip-message-on-error"));
    }

    @Test
    public void testWithTimeoutMs() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withTimeoutMs(60000L);
        assertTrue(container.getCommandOptions().contains("--timeout-ms 60000"));
    }

    @Test
    public void testWithTopic() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withTopic("test-topic");
        assertTrue(container.getCommandOptions().contains("--topic test-topic"));
    }

    @Test
    public void testWithValueDeserializer() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        assertTrue(container.getCommandOptions().contains("--value-deserializer org.apache.kafka.common.serialization.StringDeserializer"));
    }

    @Test
    public void testWithVersion() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withVersion();
        assertTrue(container.getCommandOptions().contains("--version"));
    }

    @Test
    public void testWithWhitelist() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withWhitelist(".*test.*");
        assertTrue(container.getCommandOptions().contains("--whitelist .*test.*"));
    }

    @Test
    public void testWithLogging() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withLogging();
        assertFalse(container.getLogConsumers().isEmpty());
    }

    @Test
    public void testWithKafkaVersion() {
        StrimziConsumerContainer container = new StrimziConsumerContainer()
             .withKafkaVersion("3.0.0");
        assertEquals("3.0.0", container.getKafkaVersion());
    }

    @Test
    public void testInstanceUseSharedNetwork() {
        StrimziConsumerContainer container = new StrimziConsumerContainer();
        assertEquals(container.getNetwork(), Network.SHARED);
    }
}