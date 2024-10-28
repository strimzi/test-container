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

            this.systemUnderTest = new StrimziKafkaContainer("localhost/strimzi/test-container:latest-kafka-3.8.0-arm64")
                .withOAuthConfig(
                    realmName,
                    oauthClientId,
                    oauthClientSecret,
                    keycloakAuthUri,
                    "preferred_username",
                    Arrays.asList("ANONYMOUS", "service-account-kafka-broker"))
                .withAuthenticationType(AuthenticationType.OAUTH_OVER_PLAIN)
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

    @Test
    void testOAuthBearerAuthentication() {
        final Integer keycloakPort = 8080;

        try {
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
            final String oauthClientId = "kafka-broker";
            final String oauthClientSecret = "kafka-broker-secret";

            this.systemUnderTest = new StrimziKafkaContainer("localhost/strimzi/test-container:latest-kafka-3.8.0-arm64")
                .withOAuthConfig(
                    realmName,
                    oauthClientId,
                    oauthClientSecret,
                    keycloakAuthUri,
                    "preferred_username",
                    Arrays.asList("ANONYMOUS", "service-account-kafka-broker"))
                .withAuthenticationType(AuthenticationType.OAUTH_BEARER)
                .withKraft()
                .withNetwork(Network.SHARED)
                .waitForRunning();
            this.systemUnderTest.start();

//            final int mappedKeycloakPort = this.keycloakContainer.getMappedPort(keycloakPort);

            // TODO: make sure somehow there is always same IP:HOST for issuer because it would not gonna work.
            //  because: 1) When we configure Keycloak http://keycloak:8080 (this is within docker network, which is okay with Kafka cluster)
            //           2) problem arises when clients (i.e., producer and consumer) tries to connect, because they need to use
            //              different issuer.uri http://localhost:<mapped-port-for-8080>/...
            //                    i) in /etc/hosts I have 127.0.0.1/localhost to keycloak
            //                    ii) problem is in mapped port... and so
            //          Token validation failed: Issuer not allowed: http://localhost:45595/realms/demo (ErrId: 89bec9f3)
//            String tokenEndpointUri = "http://localhost:" + mappedKeycloakPort + "/realms/demo/protocol/openid-connect/token";
//
//            // Kafka Producer Configuration with OAUTHBEARER
//            Properties producerProps = new Properties();
//            producerProps.put("bootstrap.servers", this.systemUnderTest.getBootstrapServers());
//            producerProps.put("security.protocol", "SASL_PLAINTEXT");
//            producerProps.put("sasl.mechanism", "OAUTHBEARER");
//            producerProps.put("sasl.jaas.config",
//                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
//                    "oauth.client.id=\"kafka-producer-client\" " +
//                    "oauth.client.secret=\"kafka-producer-client-secret\" " +
//                    "oauth.token.endpoint.uri=\"" + tokenEndpointUri + "\" " +
//                    "oauth.username.claim=\"preferred_username\";"
//            );
//            producerProps.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
//
//            // Additional Producer Properties
//            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
//
//            // Produce a Message
//            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("superapp_topic", "key", "value");
//            producer.send(producerRecord).get(); // Synchronous send
//            System.out.println("Produced message: key='key', value='value'");
//            producer.close();
//
//            // Kafka Consumer Configuration with OAUTHBEARER
//            Properties consumerProps = new Properties();
//            consumerProps.put("bootstrap.servers", this.systemUnderTest.getBootstrapServers());
//            consumerProps.put("security.protocol", "SASL_PLAINTEXT");
//            consumerProps.put("sasl.mechanism", "OAUTHBEARER");
//            consumerProps.put("sasl.jaas.config",
//                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
//                    "oauth.client.id=\"kafka-consumer-client\" " +
//                    "oauth.client.secret=\"kafka-consumer-client-secret\" " +
//                    "oauth.token.endpoint.uri=\"" + tokenEndpointUri + "\" " +
//                    "oauth.username.claim=\"preferred_username\";"
//            );
//            consumerProps.put("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
//
//            // Additional Consumer Properties
//            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            consumerProps.put("group.id", "my-group");
//            consumerProps.put("auto.offset.reset", "earliest");
//            consumerProps.put("enable.auto.commit", "true");
//
//            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
//            consumer.subscribe(Collections.singletonList("superapp_topic"));
//
//            // Poll for Records
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
//
//
//            // Assertions to verify that the message was received
//            assertThat(records.count(), Matchers.greaterThanOrEqualTo(1));
//
//            for (ConsumerRecord<String, String> record : records) {
//                assertThat(record.key(), CoreMatchers.is("key"));
//                assertThat(record.value(), CoreMatchers.is("value"));
//            }
//
//            consumer.close();
//        } catch (ExecutionException | InterruptedException e) {
//            throw new RuntimeException(e);
        } finally {
            if (this.keycloakContainer != null) {
                this.keycloakContainer.stop();
            }
        }
    }
}
