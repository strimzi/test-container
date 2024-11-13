/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.NetworkSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziKafkaContainerMockTest {

    private final static String KAFKA_3_9_0 = "3.9.0";

    private StrimziKafkaContainer kafkaContainer;

    @Test
    void testBuildListenersConfigSingleNetwork() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings with a single network
        Map<String, ContainerNetwork> networks = new HashMap<>();
        ContainerNetwork containerNetwork = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork.getIpAddress()).thenReturn("172.17.0.2");
        networks.put("bridge", containerNetwork);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Mocking getBootstrapServers
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092,BROKER1://0.0.0.0:9091,";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092,BROKER1://172.17.0.2:9091";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));
    }

    @Test
    void testBuildListenersConfigMultipleNetworks() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings with multiple networks
        Map<String, ContainerNetwork> networks = new LinkedHashMap<>();
        ContainerNetwork containerNetwork1 = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork1.getIpAddress()).thenReturn("172.17.0.2");
        ContainerNetwork containerNetwork2 = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork2.getIpAddress()).thenReturn("172.18.0.2");
        networks.put("bridge", containerNetwork1);
        networks.put("another_network", containerNetwork2);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Mocking getBootstrapServers
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092,BROKER1://0.0.0.0:9091,BROKER2://0.0.0.0:9090,";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092,BROKER1://172.17.0.2:9091,BROKER2://172.18.0.2:9090";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));
    }

    @Test
    void testBuildListenersConfigWithKRaft() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings
        Map<String, ContainerNetwork> networks = new HashMap<>();
        ContainerNetwork containerNetwork = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork.getIpAddress()).thenReturn("172.17.0.2");
        networks.put("bridge", containerNetwork);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Enabling KRaft mode
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };
        kafkaContainer.withKraft();

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092,BROKER1://0.0.0.0:9091,CONTROLLER://0.0.0.0:9094";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092,BROKER1://172.17.0.2:9091,CONTROLLER://localhost:9094";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));

        // Verify that listenerNames includes CONTROLLER
        assertTrue(kafkaContainer.listenerNames.contains("CONTROLLER"));
    }

    @Test
    void testBuildListenersConfigWithoutInitialListenerNames() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings
        Map<String, ContainerNetwork> networks = new HashMap<>();
        ContainerNetwork containerNetwork = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork.getIpAddress()).thenReturn("172.17.0.2");
        networks.put("bridge", containerNetwork);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Not adding any initial listener names
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092,BROKER1://0.0.0.0:9091,";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092,BROKER1://172.17.0.2:9091";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));

        // Verify that listenerNames now contains the expected listener names
        assertTrue(kafkaContainer.listenerNames.contains("PLAINTEXT"));
        assertTrue(kafkaContainer.listenerNames.contains("BROKER1"));
    }

    @Test
    void testBuildListenersConfigNoNetworks() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking empty network settings
        Map<String, ContainerNetwork> networks = new HashMap<>();
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092,";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));
    }

    @Test
    void testBuildListenersConfigWithDifferentListenerName() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings
        Map<String, ContainerNetwork> networks = new HashMap<>();
        ContainerNetwork containerNetwork = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork.getIpAddress()).thenReturn("172.17.0.2");
        networks.put("bridge", containerNetwork);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Using a different listener name (SSL)
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "SSL://localhost:9093";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "SSL://0.0.0.0:9092,BROKER1://0.0.0.0:9091,";
        String expectedAdvertisedListeners = "SSL://localhost:9093,BROKER1://172.17.0.2:9091";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));

        // Verify that listenerNames now contains the expected listener names
        assertTrue(kafkaContainer.listenerNames.contains("SSL"));
        assertTrue(kafkaContainer.listenerNames.contains("BROKER1"));
    }

    @Test
    void testBuildListenersConfigWithInvalidBootstrapServers() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking network settings
        Map<String, ContainerNetwork> networks = new HashMap<>();
        ContainerNetwork containerNetwork = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork.getIpAddress()).thenReturn("172.17.0.2");
        networks.put("bridge", containerNetwork);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        // Using invalid bootstrap servers (missing listener name)
        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "localhost:9092";
            }
        };

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> kafkaContainer.buildListenersConfig(containerInfo),
            "Expected buildListenersConfig() to throw an exception for invalid bootstrap servers."
        );

        assertThat(exception.getMessage(), containsString("must be prefixed with a listener name"));
    }

    @Test
    void testBuildListenersConfigMultipleNetworksPortNumbers() {
        // Mocking InspectContainerResponse
        InspectContainerResponse containerInfo = Mockito.mock(InspectContainerResponse.class);
        NetworkSettings networkSettings = Mockito.mock(NetworkSettings.class);
        Mockito.when(containerInfo.getNetworkSettings()).thenReturn(networkSettings);

        // Mocking multiple networks to test port number decrementing
        Map<String, ContainerNetwork> networks = new LinkedHashMap<>();
        ContainerNetwork containerNetwork1 = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork1.getIpAddress()).thenReturn("172.17.0.2");
        ContainerNetwork containerNetwork2 = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork2.getIpAddress()).thenReturn("172.18.0.2");
        ContainerNetwork containerNetwork3 = Mockito.mock(ContainerNetwork.class);
        Mockito.when(containerNetwork3.getIpAddress()).thenReturn("172.19.0.2");
        networks.put("network1", containerNetwork1);
        networks.put("network2", containerNetwork2);
        networks.put("network3", containerNetwork3);
        Mockito.when(networkSettings.getNetworks()).thenReturn(networks);

        kafkaContainer = new StrimziKafkaContainer() {
            @Override
            public String getBootstrapServers() {
                return "PLAINTEXT://localhost:9092";
            }
        };

        String[] listenersConfig = kafkaContainer.withKafkaVersion(KAFKA_3_9_0).buildListenersConfig(containerInfo);

        String expectedListeners = "PLAINTEXT://0.0.0.0:9092," +
            "BROKER1://0.0.0.0:9091," +
            "BROKER2://0.0.0.0:9090," +
            "BROKER3://0.0.0.0:9089,";
        String expectedAdvertisedListeners = "PLAINTEXT://localhost:9092," +
            "BROKER1://172.17.0.2:9091," +
            "BROKER2://172.18.0.2:9090," +
            "BROKER3://172.19.0.2:9089";

        assertThat(listenersConfig[0], is(expectedListeners));
        assertThat(listenersConfig[1], is(expectedAdvertisedListeners));

        // Verify that listenerNames includes all BROKERx
        assertTrue(kafkaContainer.listenerNames.contains("BROKER1"));
        assertTrue(kafkaContainer.listenerNames.contains("BROKER2"));
        assertTrue(kafkaContainer.listenerNames.contains("BROKER3"));
    }

    @BeforeEach
    void setUp() {
        kafkaContainer = new StrimziKafkaContainer();
    }
}
