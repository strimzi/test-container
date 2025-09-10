/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaNodeRoleTest {

    @Test
    void testMixedRoleProcessRoles() {
        assertThat(KafkaNodeRole.MIXED.getProcessRoles(), is("broker,controller"));
    }

    @Test
    void testControllerOnlyProcessRoles() {
        assertThat(KafkaNodeRole.CONTROLLER_ONLY.getProcessRoles(), is("controller"));
    }

    @Test
    void testBrokerOnlyProcessRoles() {
        assertThat(KafkaNodeRole.BROKER_ONLY.getProcessRoles(), is("broker"));
    }

    @Test
    void testMixedRoleIsController() {
        assertThat(KafkaNodeRole.MIXED.isController(), is(true));
    }

    @Test
    void testControllerOnlyIsController() {
        assertThat(KafkaNodeRole.CONTROLLER_ONLY.isController(), is(true));
    }

    @Test
    void testBrokerOnlyIsController() {
        assertThat(KafkaNodeRole.BROKER_ONLY.isController(), is(false));
    }

    @Test
    void testMixedRoleIsBroker() {
        assertThat(KafkaNodeRole.MIXED.isBroker(), is(true));
    }

    @Test
    void testControllerOnlyIsBroker() {
        assertThat(KafkaNodeRole.CONTROLLER_ONLY.isBroker(), is(false));
    }

    @Test
    void testBrokerOnlyIsBroker() {
        assertThat(KafkaNodeRole.BROKER_ONLY.isBroker(), is(true));
    }
}