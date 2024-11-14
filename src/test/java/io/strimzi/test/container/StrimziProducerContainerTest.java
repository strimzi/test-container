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

public class StrimziProducerContainerTest {

    @Test
    public void testWithBatchSize() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withBatchSize(100);
        assertTrue(container.getCommandOptions().contains("--batch-size 100"));
    }

    @Test
    public void testWithBootstrapServer() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withBootstrapServer("localhost:9092");
        assertTrue(container.getCommandOptions().contains("--bootstrap-server localhost:9092"));
    }

    @Test
    public void testWithCompressionCodecWithValue() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withCompressionCodec("gzip");
        assertTrue(container.getCommandOptions().contains("--compression-codec gzip"));
    }

    @Test
    public void testWithLineReader() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withLineReader("CustomLineReader");
        assertTrue(container.getCommandOptions().contains("--line-reader CustomLineReader"));
    }

    @Test
    public void testWithMaxBlockMs() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withMaxBlockMs(5000L);
        assertTrue(container.getCommandOptions().contains("--max-block-ms 5000"));
    }

    @Test
    public void testWithMaxMemoryBytes() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withMaxMemoryBytes(1048576L);
        assertTrue(container.getCommandOptions().contains("--max-memory-bytes 1048576"));
    }

    @Test
    public void testWithMaxPartitionMemoryBytes() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withMaxPartitionMemoryBytes(16384);
        assertTrue(container.getCommandOptions().contains("--max-partition-memory-bytes 16384"));
    }

    @Test
    public void testWithMessageSendMaxRetries() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withMessageSendMaxRetries(3);
        assertTrue(container.getCommandOptions().contains("--message-send-max-retries 3"));
    }

    @Test
    public void testWithMetadataExpiryMs() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withMetadataExpiryMs(300000L);
        assertTrue(container.getCommandOptions().contains("--metadata-expiry-ms 300000"));
    }

    @Test
    public void testWithProducerProperty() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withProducerProperty("acks", "all");
        assertTrue(container.getCommandOptions().contains("--producer-property acks=all"));
    }

    @Test
    public void testWithProducerConfig() {
        StrimziProducerContainer container = new StrimziProducerContainer()
            .withProducerConfig(MountableFile.forClasspathResource("producer.properties"));
        assertTrue(container.getCommandOptions().contains("--producer.config /tmp/producer.properties"));
    }

    @Test
    public void testWithProperty() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        assertTrue(container.getCommandOptions().contains("--property key.serializer=org.apache.kafka.common.serialization.StringSerializer"));
    }

    @Test
    public void testWithReaderConfig() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withReaderConfig(MountableFile.forClasspathResource("producer.properties"));
        assertTrue(container.getCommandOptions().contains("--reader-config /tmp/reader.properties"));
    }

    @Test
    public void testWithRequestRequiredAcks() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withRequestRequiredAcks("1");
        assertTrue(container.getCommandOptions().contains("--request-required-acks 1"));
    }

    @Test
    public void testWithRequestTimeoutMs() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withRequestTimeoutMs(3000);
        assertTrue(container.getCommandOptions().contains("--request-timeout-ms 3000"));
    }

    @Test
    public void testWithRetryBackoffMs() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withRetryBackoffMs(100L);
        assertTrue(container.getCommandOptions().contains("--retry-backoff-ms 100"));
    }

    @Test
    public void testWithSocketBufferSize() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withSocketBufferSize(1024);
        assertTrue(container.getCommandOptions().contains("--socket-buffer-size 1024"));
    }

    @Test
    public void testWithSync() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withSync();
        assertTrue(container.getCommandOptions().contains("--sync"));
    }

    @Test
    public void testWithTimeout() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withTimeout(60000L);
        assertTrue(container.getCommandOptions().contains("--timeout 60000"));
    }

    @Test
    public void testWithTopic() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withTopic("test-topic");
        assertTrue(container.getCommandOptions().contains("--topic test-topic"));
    }

    @Test
    public void testWithVersion() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withVersion();
        assertTrue(container.getCommandOptions().contains("--version"));
    }

    @Test
    public void testWithMessageContent() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withMessageContent("Hello, World!");
        assertEquals("echo \"Hello, World!\"", container.getMessageInput());
    }

    @Test
    public void testWithMessageFile() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withMessageFile(MountableFile.forClasspathResource("messages.txt"));
        assertEquals("cat /tmp/messages.txt", container.getMessageInput());
    }

    @Test
    public void testWithLogging() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withLogging();
        assertFalse(container.getLogConsumers().isEmpty());
    }

    @Test
    public void testWithKafkaVersion() {
        StrimziProducerContainer container = new StrimziProducerContainer()
             .withKafkaVersion("3.0.0");
        assertEquals("3.0.0", container.getKafkaVersion());
    }

    @Test
    public void testInstanceUseSharedNetwork() {
        StrimziProducerContainer container = new StrimziProducerContainer();
        assertEquals(container.getNetwork(), Network.SHARED);
    }
}