/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class StrimziKafkaContainerIT extends AbstractIT {

    private StrimziKafkaContainer systemUnderTest;

    @ParameterizedTest(name = "testStartContainerWithEmptyConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithEmptyConfiguration(final String imageName, final String kafkaVersion) throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new StrimziKafkaContainer(imageName)
            .withNodeId(1)
            .waitForRunning();

        systemUnderTest.start();
        assertThat(systemUnderTest.getClusterId(), notNullValue());

        String logsFromKafka = systemUnderTest.getLogs();
        assertThat(logsFromKafka, containsString("ControllerServer id=1"));
        assertThat(logsFromKafka, containsString("SocketServer listenerType=CONTROLLER, nodeId=1"));

        verify();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://" +
            systemUnderTest.getHost() + ":" + systemUnderTest.getMappedPort(9092)));
    }

    @ParameterizedTest(name = "testStartContainerWithSomeConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSomeConfiguration(final String imageName, final String kafkaVersion) throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = new StrimziKafkaContainer(imageName)
            .withNodeId(1)
            .withKafkaConfigurationMap(kafkaConfiguration)
            .waitForRunning();

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("ControllerServer id=1"));
        assertThat(logsFromKafka, containsString("SocketServer listenerType=CONTROLLER, nodeId=1"));
        assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

        verify();
    }

    @Test
    void testUnsupportedKRaftUsingKafkaVersion() {
        systemUnderTest = new StrimziKafkaContainer()
            .withKafkaVersion("2.8.2")
            .withNodeId(1)
            .waitForRunning();

        assertThrows(UnknownKafkaVersionException.class, () -> systemUnderTest.start());
    }

    @Test
    void testWithKafkaLog() {
        systemUnderTest = new StrimziKafkaContainer()
            .withNodeId(1)
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

    private void verify() throws InterruptedException, ExecutionException, TimeoutException {
        final String topicName = "topic";

        Map<String, Object> producerConfigs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers());

        Map<String, Object> consumerConfigs = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "my-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // using try-with-resources for KafkaProducer, KafkaConsumer and Admin (implicit closing connection)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfigs, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs, new StringDeserializer(), new StringDeserializer());
             final Admin admin = Admin.create(Map.of(
                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {
            final List<NewTopic> topics = List.of(new NewTopic(topicName, 1, (short) 1));
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicName, "some-key", "1")).get();
            producer.send(new ProducerRecord<>(topicName, "some-key", "2")).get();
            producer.send(new ProducerRecord<>(topicName, "some-key", "3")).get();
            TopicPartition topic = new TopicPartition(topicName, 0);
            consumer.assign(Set.of(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            assertThat(records.count(), equalTo(3));
            assertThat(records.records(topic).get(0).value(), equalTo("1"));
            assertThat(records.records(topic).get(1).value(), equalTo("2"));
            assertThat(records.records(topic).get(2).value(), equalTo("3"));
        }
    }

    @Test
    void testLogCollectionWithDefaultPath() throws IOException {
        String expectedLogFilePath = "target/strimzi-test-container-logs/kafka-container-1.log";
        Path logPath = Paths.get(expectedLogFilePath);

        // Clean up any existing log file
        Files.deleteIfExists(logPath);

        systemUnderTest = new StrimziKafkaContainer()
            .withNodeId(1)
            .withLogCollection()
            .waitForRunning();

        systemUnderTest.start();

        // Verify the container is running and has logs
        assertThat(systemUnderTest.getLogs(), containsString("ControllerServer id=1"));

        systemUnderTest.stop();

        // Verify log file was created
        assertThat("Log file should exist", Files.exists(logPath), is(true));

        // Verify log file contains expected content
        String logContent = Files.readString(logPath);
        assertThat(logContent, containsString("ControllerServer id=1"));

        // Clean up
        Files.deleteIfExists(logPath);
    }


    @Test
    void testLogCollectionWithCustomPath() throws IOException {
        String customLogPath = "test-logs/custom-kafka.log";
        Path logPath = Paths.get(customLogPath);

        // Clean up any existing log file
        Files.deleteIfExists(logPath);

        systemUnderTest = new StrimziKafkaContainer()
            .withNodeId(1)
            .withLogCollection(customLogPath)
            .waitForRunning();

        systemUnderTest.start();

        // Verify the container is running and has logs
        assertThat(systemUnderTest.getLogs(), containsString("ControllerServer id=1"));

        systemUnderTest.stop();

        // Verify log file was created at custom path
        assertThat("Custom log file should exist", Files.exists(logPath), is(true));

        // Verify log file contains expected content
        String logContent = Files.readString(logPath);
        assertThat(logContent, containsString("ControllerServer id=1"));

        // Clean up
        Files.deleteIfExists(logPath);
    }

    @AfterEach
    void afterEach() {
        if (this.systemUnderTest != null) {
            this.systemUnderTest.stop();
        }
    }
}
