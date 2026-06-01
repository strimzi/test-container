/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Represents a Kafka listener with its name and role.
 * Defines listener name constants to avoid magic strings across the codebase.
 */
class Listener {

    static final String PLAINTEXT = "PLAINTEXT";
    static final String SSL = "SSL";
    static final String CONTROLLER = "CONTROLLER";
    static final String CONTROLLER_EXTERNAL = "CONTROLLER_EXTERNAL";
    static final String INTER_BROKER_PREFIX = "BROKER";

    /**
     * Defines the role of a Kafka listener.
     */
    enum Role {
        /** Client-facing listener for external connections */
        CLIENT,
        /** Inter-broker communication listener */
        INTER_BROKER,
        /** Controller communication listener (internal, Docker network) */
        CONTROLLER,
        /** Controller communication listener (external, host-accessible) */
        CONTROLLER_EXTERNAL
    }

    private final String name;
    private final Role role;

    Listener(String name, Role role) {
        this.name = name;
        this.role = role;
    }

    public String name() {
        return name;
    }

    public Role role() {
        return role;
    }
}
