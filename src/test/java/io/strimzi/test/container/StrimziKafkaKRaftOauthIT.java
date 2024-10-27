/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaKRaftOauthIT extends AbstractIT {

    private StrimziKafkaContainer systemUnderTest;
    private static final String KEYCLOAK_NETWORK_ALIAS = "keycloak";
    private KeycloakContainer keycloakContainer;

    @Test
    void testOAuthOverPlain() {
        try {
            final Integer keycloakPort = 8080;

            this.keycloakContainer = new KeycloakContainer()
                .withRealmImportFile("/demo-realm.json")
                .withEnv("KEYCLOAK_ADMIN", "admin")
                .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
                .withExposedPorts(keycloakPort)
                .withNetwork(Network.SHARED)
                .withNetworkAliases(KEYCLOAK_NETWORK_ALIAS)
                .waitingFor(Wait.forHttp("/realms/master").forStatusCode(200).forPort(keycloakPort));

            this.keycloakContainer.start();

            final String realmName = "demo";
            final String keycloakAuthUri = "http://"  + KEYCLOAK_NETWORK_ALIAS + ":" + keycloakPort;
            final String oauthClientId = "kafka";
            final String oauthClientSecret = "kafka-secret";

            this.systemUnderTest = new StrimziKafkaContainer("localhost/strimzi/test-container:latest-kafka-3.8.0-amd64")
                .withOAuthConfig(
                    realmName,
                    oauthClientId,
                    oauthClientSecret,
                    keycloakAuthUri,
                    Arrays.asList("ANONYMOUS", "service-account-kafka-broker"))
                .withOAuthOverPlain()
                .withSaslUsername("kafka-broker")
                .withSaslPassword("kafka-broker-secret")
                .withKraft()
                .withNetwork(Network.SHARED)
                .waitForRunning();
            this.systemUnderTest.start();

            final Properties producerProps = new Properties();
            // Additional producer properties
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producerProps.put("bootstrap.servers", this.systemUnderTest.getBootstrapServers());
            producerProps.put("security.protocol", "SASL_PLAINTEXT");
            producerProps.put("sasl.mechanism", "PLAIN");
            producerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"kafka-producer-client\" " +
                    "password=\"kafka-producer-client-secret\";"
            );

            // OAuth related
            producerProps.put("oauth.token.endpoint.uri", "http://keycloak:8080/realms/demo/protocol/openid-connect/token");
            producerProps.put("oauth.client.id", "kafka-producer-client");
            producerProps.put("oauth.client.secret", "kafka-producer-client-secret");
            producerProps.put("oauth.username.claim", "preferred_username");

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("superapp_topic", "key", "value");
            producer.send(producerRecord);
            producer.close();

            final Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", this.systemUnderTest.getBootstrapServers());
            consumerProps.put("security.protocol", "SASL_PLAINTEXT");
            consumerProps.put("sasl.mechanism", "PLAIN");
            consumerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"kafka-consumer-client\" " +
                    "password=\"kafka-consumer-client-secret\";"
            );

            // OAuth related
            consumerProps.put("oauth.token.endpoint.uri", "http://keycloak:8080/realms/demo/protocol/openid-connect/token");
            consumerProps.put("oauth.client.id", "kafka-consumer-client");
            consumerProps.put("oauth.client.secret", "kafka-consumer-client-secret");
            consumerProps.put("oauth.username.claim", "preferred_username");

            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("group.id", "my-group");
            consumerProps.put("auto.offset.reset", "earliest");

            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList("superapp_topic"));

            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            // Assertions to verify that the message was received
            assertThat(records.count(), Matchers.greaterThanOrEqualTo(1));

            for (ConsumerRecord<String, String> record : records) {
                assertThat(record.key(), CoreMatchers.is("key"));
                assertThat(record.value(), CoreMatchers.is("value"));
            }

            consumer.close();
        } finally {
            if (this.keycloakContainer != null) {
                this.keycloakContainer.stop();
            }
            if (this.systemUnderTest != null) {
                this.systemUnderTest.stop();
            }
        }
    }
}
