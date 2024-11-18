/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.util.ArrayList;
import java.util.List;

import eu.rekawek.toxiproxy.Proxy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.CoreMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.testcontainers.utility.DockerImageName;

@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class StrimziKafkaClusterIT extends AbstractIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKafkaContainerIT.class);
    private static final int NUMBER_OF_REPLICAS = 3;

    private StrimziKafkaCluster systemUnderTest;

    @Test
    void testKafkaClusterStartup() throws IOException, InterruptedException {
        setUpKafkaCluster();

        verifyReadinessOfKafkaCluster();
    }

    @Test
    void testKafkaClusterStartupWithSharedNetwork() throws IOException, InterruptedException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .withSharedNetwork()
            .build();
        systemUnderTest.start();

        verifyReadinessOfKafkaCluster();
    }

    @Test
    void testKafkaClusterFunctionality() throws ExecutionException, InterruptedException, TimeoutException {
        setUpKafkaCluster();

        verifyFunctionalityOfKafkaCluster();
    }

    @Test
    void testKafkaClusterWithSharedNetworkFunctionality() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .withSharedNetwork()
            .build();
        systemUnderTest.start();

        verifyFunctionalityOfKafkaCluster();
    }

    @Test
    void testStartClusterWithProxyContainer() {
        ToxiproxyContainer proxyContainer = new ToxiproxyContainer(
                DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")
                        .asCompatibleSubstituteFor("shopify/toxiproxy"));

        try {
            systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(NUMBER_OF_REPLICAS)
                .withProxyContainer(proxyContainer)
                .build();

            systemUnderTest.start();
            List<String> bootstrapUrls = new ArrayList<>();
            for (KafkaContainer kafkaContainer : systemUnderTest.getBrokers()) {
                Proxy proxy = ((StrimziKafkaContainer) kafkaContainer).getProxy();
                assertThat(proxy, notNullValue());
                bootstrapUrls.add(kafkaContainer.getBootstrapServers());
            }

            assertThat(systemUnderTest.getBootstrapServers(),
                is(String.join(",", bootstrapUrls)));
        } finally {
            systemUnderTest.stop();
        }
    }

    private void setUpKafkaCluster() {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(NUMBER_OF_REPLICAS)
            .build();
        systemUnderTest.start();
    }

    private void verifyReadinessOfKafkaCluster() throws IOException, InterruptedException {
        // exercise (fetch the data)
        final Container.ExecResult result = this.systemUnderTest.getZookeeper().execInContainer(
            "sh", "-c",
            "bin/zookeeper-shell.sh zookeeper:" + StrimziZookeeperContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
        );

        final String brokers = result.getStdout();
        // verify that all kafka brokers are bind to zookeeper
        assertThat(brokers, notNullValue());
        // three brokers
        assertThat(brokers, CoreMatchers.containsString("[0, 1, 2]"));

        LOGGER.info("Brokers are {}", systemUnderTest.getBootstrapServers());
    }

    private void verifyFunctionalityOfKafkaCluster() throws ExecutionException, InterruptedException, TimeoutException {
        // using try-with-resources for AdminClient, KafkaProducer and KafkaConsumer (implicit closing connection)
        try (final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()));
             KafkaProducer<String, String> producer = new KafkaProducer<>(
                 ImmutableMap.of(
                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                     ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                 ),
                 new StringSerializer(),
                 new StringSerializer()
             );
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                 ImmutableMap.of(
                     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                     ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT)
                 ),
                 new StringDeserializer(),
                 new StringDeserializer()
             )
        ) {
            final String topicName = "example-topic";
            final String recordKey = "strimzi";
            final String recordValue = "the-best-project-in-the-world";

            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, NUMBER_OF_REPLICAS, (short) NUMBER_OF_REPLICAS));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, recordKey, recordValue)).get();

            Utils.waitFor("Consumer records are present", Duration.ofSeconds(10), Duration.ofMinutes(2),
                () -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    if (records.isEmpty()) {
                        return false;
                    }

                    // verify count
                    assertThat(records.count(), is(1));

                    ConsumerRecord<String, String> consumerRecord = records.records(topicName).iterator().next();

                    // verify content of the record
                    assertThat(consumerRecord.topic(), is(topicName));
                    assertThat(consumerRecord.key(), is(recordKey));
                    assertThat(consumerRecord.value(), is(recordValue));

                    return true;
                });
        }
    }

    @AfterEach
    void tearDown() {
        if (systemUnderTest != null) {
            systemUnderTest.stop();
        }
    }
}
