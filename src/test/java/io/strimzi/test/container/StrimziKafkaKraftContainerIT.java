/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
public class StrimziKafkaKraftContainerIT extends AbstractIT {

    private StrimziKafkaContainer systemUnderTest;

    @ParameterizedTest(name = "testStartContainerWithEmptyConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithEmptyConfiguration(final String imageName, final String kafkaVersion) throws ExecutionException, InterruptedException, TimeoutException {
        supportsKraftMode(imageName);

        try {
            systemUnderTest = new StrimziKafkaContainer(imageName)
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();

            systemUnderTest.start();
            assertThat(systemUnderTest.getClusterId(), notNullValue());

            String logsFromKafka = systemUnderTest.getLogs();
            if (isLessThanKafka350(kafkaVersion)) {
                assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));
            } else {
                assertThat(logsFromKafka, containsString("ControllerServer id=1"));
                assertThat(logsFromKafka, containsString("SocketServer listenerType=CONTROLLER, nodeId=1"));
            }

            verify();

            assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://" +
                systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));
        } finally {
            systemUnderTest.stop();
        }
    }

    @ParameterizedTest(name = "testStartContainerWithSomeConfiguration-{0}")
    @MethodSource("retrieveKafkaVersionsFile")
    void testStartContainerWithSomeConfiguration(final String imageName, final String kafkaVersion) throws ExecutionException, InterruptedException, TimeoutException {
        supportsKraftMode(imageName);
        try {
            Map<String, String> kafkaConfiguration = new HashMap<>();

            kafkaConfiguration.put("log.cleaner.enable", "false");
            kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
            kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
            kafkaConfiguration.put("log.index.interval.bytes", "2048");

            systemUnderTest = new StrimziKafkaContainer(imageName)
                .withBrokerId(1)
                .withKraft()
                .withKafkaConfigurationMap(kafkaConfiguration)
                .waitForRunning();

            systemUnderTest.start();

            String logsFromKafka = systemUnderTest.getLogs();

            if (isLessThanKafka350(kafkaVersion)) {
                assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));
            } else {
                assertThat(logsFromKafka, containsString("ControllerServer id=1"));
                assertThat(logsFromKafka, containsString("SocketServer listenerType=CONTROLLER, nodeId=1"));
            }
            assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
            assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
            assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
            assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

            verify();
        } finally {
            systemUnderTest.stop();
        }
    }

    @Test
    void testUnsupportedKRaftUsingKafkaVersion() {
        try {
            systemUnderTest = new StrimziKafkaContainer()
                .withKafkaVersion("2.8.2")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();

            assertThrows(UnsupportedKraftKafkaVersionException.class, () -> systemUnderTest.start());
        } finally {
            systemUnderTest.stop();
        }

    }

    @Test
    void testUnsupportedKRaftUsingImageName() {
        try {
            systemUnderTest = new StrimziKafkaContainer("quay.io/strimzi-test-container/test-container:latest-kafka-2.8.2")
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();

            assertThrows(UnsupportedKraftKafkaVersionException.class, () -> systemUnderTest.start());
        } finally {
            systemUnderTest.stop();

        }
    }

    @Test
    void testWithKafkaLog() {
        systemUnderTest = new StrimziKafkaContainer()
            .waitForRunning()
            .withKraft()
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

        systemUnderTest.stop();
    }

    private void verify() throws InterruptedException, ExecutionException, TimeoutException {
        final String topicName = "topic";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());
        consumerProperties.put("group.id", "my-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        // using try-with-resources for KafkaProducer, KafkaConsumer and AdminClient (implicit closing connection)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
             final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {
            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicName, "some-key", "1")).get();
            producer.send(new ProducerRecord<>(topicName, "some-key", "2")).get();
            producer.send(new ProducerRecord<>(topicName, "some-key", "3")).get();
            TopicPartition topic = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            assertThat(records.count(), equalTo(3));
            assertThat(records.records(topic).get(0).value(), equalTo("1"));
            assertThat(records.records(topic).get(1).value(), equalTo("2"));
            assertThat(records.records(topic).get(2).value(), equalTo("3"));
        }
    }

    @AfterEach
    void afterEach() {
        if (this.systemUnderTest != null) {
            this.systemUnderTest.stop();
        }
    }
}
