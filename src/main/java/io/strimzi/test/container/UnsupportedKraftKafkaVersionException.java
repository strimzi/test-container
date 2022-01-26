/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Provides a handler, when user specifies unsupported Kafka version with Kraft mode enabled
 * {@link StrimziKafkaContainer#withKraft()}.
 */
public class UnsupportedKraftKafkaVersionException extends RuntimeException {

    /**
     * {@link UnsupportedKraftKafkaVersionException} used for handling situation, when
     * user specifies unsupported Kafka version with Kraft mode enabled.
     *
     * @param message specific message to throw
     */
    public UnsupportedKraftKafkaVersionException(String message) {
        super(message);
    }

    /**
     * {@link UnsupportedKraftKafkaVersionException} used for handling situation, when
     * user specifies unsupported Kafka version with Kraft mode enabled.
     *
     * @param cause specific cause to throw
     */
    public UnsupportedKraftKafkaVersionException(Throwable cause) {
        super(cause);
    }
}
