/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Enum representing authentication types for StrimziKafkaContainer.
 */
public enum AuthenticationType {
    OAUTH_OVER_PLAIN,
    OAUTH_BEARER,
    MUTUAL_TLS,
    SCRAM_SHA_256,
    SCRAM_SHA_512,
    GSSAPI;
}