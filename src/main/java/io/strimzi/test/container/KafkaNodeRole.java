/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

/**
 * Internal: Enum representing the different roles a Kafka node can have in KRaft mode.
 */
enum KafkaNodeRole {
    /**
     * Mixed-role node that acts as both broker and controller.
     * This is the default behavior and maintains backward compatibility.
     */
    MIXED("broker,controller"),

    /**
     * Controller-only node that participates in the metadata quorum
     * but does not handle client requests or store topic data.
     */
    CONTROLLER_ONLY("controller"),

    /**
     * Broker-only node that handles client requests and stores topic data
     * but does not participate in the metadata quorum.
     */
    BROKER_ONLY("broker");

    private final String processRoles;

    KafkaNodeRole(String processRoles) {
        this.processRoles = processRoles;
    }

    /**
     * Returns the Kafka process.roles configuration value for this node role.
     *
     * @return the process.roles configuration string
     */
    public String getProcessRoles() {
        return processRoles;
    }

    /**
     * Checks if this node role includes controller functionality.
     *
     * @return true if the node can act as a controller, false otherwise
     */
    public boolean isController() {
        return this == MIXED || this == CONTROLLER_ONLY;
    }

    /**
     * Checks if this node role includes broker functionality.
     *
     * @return true if the node can act as a broker, false otherwise
     */
    public boolean isBroker() {
        return this == MIXED || this == BROKER_ONLY;
    }
}