/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Enum representing authentication types for {@link StrimziKafkaContainer}.
 */
public enum AuthenticationType {

    /**
     * OAuth authentication over plain text.
     */
    OAUTH_OVER_PLAIN,

    /**
     * OAuth Bearer token authentication.
     */
    OAUTH_BEARER,

    /**
     * Mutual TLS authentication.
     */
    MUTUAL_TLS,

    /**
     * SCRAM-SHA-256 authentication.
     */
    SCRAM_SHA_256,

    /**
     * SCRAM-SHA-512 authentication.
     */
    SCRAM_SHA_512,

    /**
     * GSSAPI (Kerberos) authentication.
     */
    GSSAPI;
}