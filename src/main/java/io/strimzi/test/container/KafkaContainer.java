/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.testcontainers.lifecycle.Startable;

public interface KafkaContainer extends Startable {
    /**
     * Check if the cluster is running in KRaft mode or it is using an external ZK.
     *
     * @return true only in KRaft mode or when using an external ZK
     */
    boolean hasKraftOrExternalZooKeeperConfigured();

    /**
     * Get the internal ZooKeeper connect string.
     *
     * @return ZooKeeper connect string
     *
     * @throws IllegalStateException
     *      if in KRaft mode or using an external ZooKeeper
     */
    String getInternalZooKeeperConnect();

    /**
     * Get the Kafka cluster bootstrap servers.
     *
     * @return bootstrap servers
     */
    String getBootstrapServers();
}
