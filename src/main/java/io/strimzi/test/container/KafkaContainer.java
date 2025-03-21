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
     * Check if the cluster is running in KRaft mode or it is using an external ZK.
     *
     * @return true only in KRaft mode or when using an external ZK
     */
    boolean hasKraftOrExternalZooKeeperConfigured();

    /**
     * Get the Kafka cluster bootstrap servers.
     *
     * @return bootstrap servers
     */
    String getBootstrapServers();
}
