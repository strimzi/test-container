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
        assertThat(KafkaNodeRole.COMBINED.getProcessRoles(), is("broker,controller"));
    }

    @Test
    void testControllerOnlyProcessRoles() {
        assertThat(KafkaNodeRole.CONTROLLER.getProcessRoles(), is("controller"));
    }

    @Test
    void testBrokerOnlyProcessRoles() {
        assertThat(KafkaNodeRole.BROKER.getProcessRoles(), is("broker"));
    }

    @Test
    void testMixedRoleIsController() {
        assertThat(KafkaNodeRole.COMBINED.isController(), is(true));
    }

    @Test
    void testControllerOnlyIsController() {
        assertThat(KafkaNodeRole.CONTROLLER.isController(), is(true));
    }

    @Test
    void testBrokerOnlyIsController() {
        assertThat(KafkaNodeRole.BROKER.isController(), is(false));
    }

    @Test
    void testMixedRoleIsBroker() {
        assertThat(KafkaNodeRole.COMBINED.isBroker(), is(true));
    }

    @Test
    void testControllerOnlyIsBroker() {
        assertThat(KafkaNodeRole.CONTROLLER.isBroker(), is(false));
    }

    @Test
    void testBrokerOnlyIsBroker() {
        assertThat(KafkaNodeRole.BROKER.isBroker(), is(true));
    }
}