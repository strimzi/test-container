/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.testcontainers.lifecycle.Startable;

/**
 * {@code KafkaContainer} is an interface that represents a Kafka container.
 * It extends the {@link Startable} interface, allowing Kafka nodes to be started and stopped.
 * <p>
 * Implementations of this interface should provide mechanisms to manage Kafka node instances,
 * including configuration and connection details necessary for integration and system testing.
 * Kafka nodes can act as brokers, controllers, or combined-role nodes depending on their configuration.
 * </p>
 */
public interface KafkaContainer extends Startable {
    /**
     * Get the Kafka cluster bootstrap servers.
     *
     * @return bootstrap servers
     */
    String getBootstrapServers();

    /**
     * Get the Kafka controller bootstrap servers for admin operations that require controller access.
     * This is particularly useful in KRaft mode for operations like cluster metadata queries.
     *
     * @return controller bootstrap servers
     */
    String getBootstrapControllers();
}
