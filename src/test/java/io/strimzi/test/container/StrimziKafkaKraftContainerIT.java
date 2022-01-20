/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class StrimziKafkaKraftContainerIT {

    private StrimziKafkaContainer systemUnderTest;

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testStartContainerWithEmptyConfiguration() throws ExecutionException, InterruptedException {
        assumeDocker();

        try {
            systemUnderTest = new StrimziKafkaContainer()
                .withBrokerId(1)
                .withKraft()
                .waitForRunning();

            systemUnderTest.start();

            String logsFromKafka = systemUnderTest.getLogs();
            assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));

            verify();

            assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://" +
                systemUnderTest.getContainerIpAddress() + ":" + systemUnderTest.getMappedPort(9092)));
        } finally {
            systemUnderTest.stop();
        }
    }

    @Test
    void testStartContainerWithSomeConfiguration() throws ExecutionException, InterruptedException {
        assumeDocker();

        try {
            Map<String, String> kafkaConfiguration = new HashMap<>();

            kafkaConfiguration.put("log.cleaner.enable", "false");
            kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
            kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
            kafkaConfiguration.put("log.index.interval.bytes", "2048");

            systemUnderTest = new StrimziKafkaContainer()
                .withBrokerId(1)
                .withKraft()
                .withKafkaConfigurationMap(kafkaConfiguration)
                .waitForRunning();

            systemUnderTest.start();

            String logsFromKafka = systemUnderTest.getLogs();

            assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));
            assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
            assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
            assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
            assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

            verify();
        } finally {
            systemUnderTest.stop();
        }
    }

    private void verify() throws InterruptedException, ExecutionException {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());
        consumerProperties.put("group.id", "my-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        // using try-with-resources for KafkaProducer and KafkaConsumer (implicit closing connection)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
        ){
            producer.send(new ProducerRecord<>("topic", "some-key", "1")).get();
            producer.send(new ProducerRecord<>("topic", "some-key", "2")).get();
            producer.send(new ProducerRecord<>("topic", "some-key", "3")).get();
            TopicPartition topic = new TopicPartition("topic", 0);
            consumer.assign(Collections.singleton(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            assertThat(records.count(), equalTo(3));
            assertThat(records.records(topic).get(0).value(), equalTo("1"));
            assertThat(records.records(topic).get(1).value(), equalTo("2"));
            assertThat(records.records(topic).get(2).value(), equalTo("3"));
        }

    }
}
