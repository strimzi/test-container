/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container.utils;

import io.strimzi.test.container.KafkaVersionService;

/**
 * Provides a handler, when {@link KafkaVersionService#strimziTestContainerImageName(String)} does not know the concrete
 * Kafka version.
 */
public class UnknownKafkaVersionException extends RuntimeException {

    /**
     * UnknownKafkaVersionException used for handling situation, where
     * {@link KafkaVersionService#strimziTestContainerImageName(String)} can not find Kafka version
     *
     * @param message specific message to throw
     */
    public UnknownKafkaVersionException(String message) {
        super(message);
    }

    /**
     * UnknownKafkaVersionException used for handling situation, where
     * {@link KafkaVersionService#strimziTestContainerImageName(String)} can not find Kafka version
     *
     * @param cause specific cause to throw
     */
    public UnknownKafkaVersionException(Throwable cause) {
        super(cause);
    }
}
