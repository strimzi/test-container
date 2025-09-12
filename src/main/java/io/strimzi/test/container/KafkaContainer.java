/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.testcontainers.lifecycle.Startable;

/**
 * {@code KafkaContainer} is an interface that represents a Kafka broker container.
 * It extends the {@link Startable} interface, allowing Kafka brokers to be started and stopped.
 * <p>
 * Implementations of this interface should provide mechanisms to manage Kafka broker instances,
 * including configuration and connection details necessary for integration and system testing.
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
     * @throws UnsupportedOperationException if the implementation doesn't support controller endpoints
     */
    default String getBootstrapControllers() {
        throw new UnsupportedOperationException("Controller endpoints are not supported by this implementation");
    }
}
