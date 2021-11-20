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

public class StrimziKafkaKraftContainerIT {

    private StrimziKafkaContainer systemUnderTest;

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testStartContainerWithEmptyConfiguration() throws ExecutionException, InterruptedException {
        assumeDocker();
        systemUnderTest = StrimziKafkaContainer.createWithKraft(1)
                        .waitForRunning();

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();
        assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));

        verify();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://localhost:" + systemUnderTest.getMappedPort(9092)));
    }

    @Test
    void testStartContainerWithSomeConfiguration() throws ExecutionException, InterruptedException {
        assumeDocker();

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = StrimziKafkaContainer.createWithAdditionalConfiguration(1, true, kafkaConfiguration)
                .withStorageUUID("xtzWWN5bTjitdL4efd9g6g")
                .waitForRunning();

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("RaftManager nodeId=1"));
        assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

        verify();

        systemUnderTest.stop();
    }

    private void verify() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        producer.send(new ProducerRecord<>("topic", "some-key", "1")).get();
        producer.send(new ProducerRecord<>("topic", "some-key", "2")).get();
        producer.send(new ProducerRecord<>("topic", "some-key", "3")).get();

        properties.put("group.id", "my-group");
        properties.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        TopicPartition topic = new TopicPartition("topic", 0);
        consumer.assign(Collections.singleton(topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
        assertThat(records.count(), equalTo(3));
        assertThat(records.records(topic).get(0).value(), equalTo("1"));
        assertThat(records.records(topic).get(1).value(), equalTo("2"));
        assertThat(records.records(topic).get(2).value(), equalTo("3"));
    }
}
