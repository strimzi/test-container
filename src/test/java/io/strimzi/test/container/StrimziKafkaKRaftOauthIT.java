/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StrimziKafkaKRaftOauthIT extends AbstractIT {

    private StrimziKafkaContainer systemUnderTest;
    private static final String KEYCLOAK_NETWORK_ALIAS = "keycloak";
    private KeycloakContainer keycloakContainer;

    @Test
    void testOAuthOverPlain() {
        try {
            this.keycloakContainer = new KeycloakContainer()
                .withRealmImportFile("/demo-realm.json")
                .withEnv("KEYCLOAK_ADMIN", "admin")
                .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
                .withExposedPorts(8080)
                .withNetwork(Network.SHARED)
                .withNetworkAliases(KEYCLOAK_NETWORK_ALIAS)
                .waitingFor(Wait.forHttp("/realms/master").forStatusCode(200).forPort(8080));

            this.keycloakContainer.start();

            final Integer keycloakPort = 8080;
            final String realmName = "demo";
            final String keycloakAuthUri = "http://"  + KEYCLOAK_NETWORK_ALIAS + ":" + keycloakPort;

            Map<String, String> additionalConfiguration = new HashMap<>();

            additionalConfiguration.put("sasl.enabled.mechanisms", "PLAIN");

            // requirement failed: sasl.mechanism.inter.broker.protocol must be included in sasl.enabled.mechanisms when SASL is used for inter-broker communication
            additionalConfiguration.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
            additionalConfiguration.put("listener.security.protocol.map", "PLAINTEXT:SASL_PLAINTEXT,BROKER1:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT");

            // �[2024-10-25 08:35:57,325] ERROR Encountered fatal fault: caught exception (org.apache.kafka.server.fault.ProcessTerminatingFaultHandler)
            // �java.lang.IllegalArgumentException: Could not find a 'KafkaServer' or 'controller.KafkaServer' entry in the JAAS configuration. System property 'java.security.auth.login.config' is not set
            additionalConfiguration.put("sasl.mechanism.controller.protocol", "PLAIN");

            additionalConfiguration.put("listener.name.plaintext.plain.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-broker\" password=\"kafka-broker-secret\";");
            additionalConfiguration.put("listener.name.controller.plain.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-broker\" password=\"kafka-broker-secret\";");
            additionalConfiguration.put("listener.name.broker1.plain.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka-broker\" password=\"kafka-broker-secret\";");

            // Caused by: javax.security.sasl.SaslException: Cannot get userid/password [Caused by javax.security.auth.callback.UnsupportedCallbackException: Could not login: the client is being asked for a password, but the Kafka client code does not currently support obtaining a password from the user.]
            additionalConfiguration.put("listener.name.plaintext.plain.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler");
            additionalConfiguration.put("listener.name.broker1.plain.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler");
            additionalConfiguration.put("listener.name.controller.plain.sasl.server.callback.handler.class", "io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler");

            // Principal builder
            additionalConfiguration.put("principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder");

            // Super users
            additionalConfiguration.put("super.users",
                "User:ANONYMOUS;User:service-account-kafka-broker");

            this.systemUnderTest = new StrimziKafkaContainer("localhost/strimzi/test-container:latest-kafka-3.8.0-amd64")
                .withKraft()
                .withKafkaConfigurationMap(additionalConfiguration)
                .withNetwork(Network.SHARED)
                .withEnv("KAFKA_DEBUG", "y")
                .withKafkaLog(Level.INFO)
                // org.apache.kafka.common.KafkaException: io.strimzi.kafka.oauth.common.ConfigException:
                // OAuth validator configuration error: either 'oauth.jwks.endpoint.uri' (for fast local signature validation) or
                // 'oauth.introspection.endpoint.uri' (for using authorization server during validation) should be specified!
                // for now we would use FAST LOCAL SIGNATURE VALIDATION (quicker) -> no need to call remotely for Keycloak (only once or when tokens expires)
                // we have to configure OAuth relatead stuff as ENVs because we are unable to set SystemProperties easily
                .withEnv("OAUTH_JWKS_ENDPOINT_URI", keycloakAuthUri + "/realms/" +  realmName + "/protocol/openid-connect/certs")
                // org.apache.kafka.common.KafkaException: io.strimzi.kafka.oauth.common.ConfigException:
                //  OAuth validator configuration error: 'oauth.valid.issuer.uri' must be set or 'oauth.check.issuer' has to be set to 'false'
                .withEnv("OAUTH_VALID_ISSUER_URI", keycloakAuthUri + "/realms/" + realmName)
                // �Caused by: io.strimzi.kafka.oauth.services.ServiceException: Failed to fetch public keys needed to validate JWT signatures: http://localhost:41951/auth/realms/kafka-authz/protocol/openid-connect/certs
                //  Failed to fetch public keys needed to validate JWT signatures: http://localhost:36247/auth/realms/kafka-authz/protocol/openid-connect/certs
                .withEnv("OAUTH_USERNAME_CLAIM", "preferred_username")
                .withEnv("OAUTH_CLIENT_ID", "kafka")
                .withEnv("OAUTH_CLIENT_SECRET", "kafka-secret")
                .withEnv("OAUTH_TOKEN_ENDPOINT_URI", keycloakAuthUri + "/realms/" + realmName + "/protocol/openid-connect/token")
                .waitForRunning();
            this.systemUnderTest.start();

            Properties producerProps = new Properties();
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

            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("superapp_topic", "key", "value");
            producer.send(producerRecord);
            producer.close();

            Properties consumerProps = new Properties();
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

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList("superapp_topic"));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            records.forEach(record -> {
                System.out.printf("Received message: key=%s, value=%s%n", record.key(), record.value());
            });
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
