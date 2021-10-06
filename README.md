[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# Strimzi Template repository

The test container repository primarily deals with developing and maintaining test container code that uses an image from the [strimzi/test-container-images](https://github.com/strimzi/test-container-images) repository. 
The main dependency is the test container framework, which offers the overall life of the docker container.

## Why to use?

Since this is a simple encapsulation of Kafka binaries in the docker image, it is evident that the overall spin-up of such a Kafka container will be pretty swift. 
Therefore, it is a suitable candidate for the unit or integration testing. If the user needs something more complex, there is a multi-node Kafka cluster implementation with only infrastructure limitations. 
Furthermore, for accuracy, we will describe some of the most important classes:
- `StrimziKafkaContainer` is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the given version. 
  There are two options for how to use it. The first one is using an embedded zookeeper which will run inside Kafka container. 
  Another option is to use @StrimziZookeeperContainer as an external Zookeeper. 
  The additional configuration for Kafka Broker can be injected via the constructor. 
  This container is a good fit for integration testing, but for more hardcore testing, we suggest using @StrimziKafkaCluster.
- `StrimziZookeeperContainer` is an instance of the Zookeeper encapsulated inside a docker container using an image from quay.io/strimzi/kafka with the given version. 
  It can be combined with @StrimziKafkaContainer, but we suggest using directly @StrimziKafkaCluster for more complicated testing.
- `StrimziKafkaCluster` is a multi-node instance of Kafka and Zookeeper using the latest image from quay.io/strimzi/kafka with the given version. 
  It perfectly fits for integration/system testing. We always deploy one Zookeeper with a specified number of Kafka instances, running as a separate container inside Docker. 
  The additional configuration for Kafka brokers can be passed to the constructor.
  
## How to use?

Using a given test container is very simple. 
First, the user needs to specify two essential env variables:

- `STRIMZI_TEST_CONTAINER_KAFKA_VERSION` - specification of the Kafka version with which it wants the given container to run (i.e., `3.0.0`)
- `STRIMZI_TEST_CONTAINER_IMAGE_VERSION` - specification Strimzi test container image, which was built in the test-container-images repositories (i.e., `0.1.0`)

At the same time, in case of using the given dependency, it is possible to add it to the project as a Maven dependency:

```
// in case of 0.26.0 version
<dependency>
    <groupId>io.strimzi.test.container</groupId>
    <artifactId>strimzi-test-container</artifactId>
    <version>0.26.0</version>
</dependency>
```

For instance, we can then run the given Kafka container with additional Kafka configuration from Java code
(we assume that you already specified `STRIMZI_TEST_CONTAINER_KAFKA_VERSION` and `STRIMZI_TEST_CONTAINER_IMAGE_VERSION` environment variables):
```java
final int brokerId = 1;

// additional configuration
Map<String, String> additionalKafkaConfiguration = Map.of(
    "log.cleaner.enable", "false", 
    "log.cleaner.backoff.ms", 
    "1000", "ssl.enabled.protocols", "TLSv1", 
    "log.index.interval.bytes", "2048"
);


StrimziKafkaContainer strimziKafkaContainer = StrimziKafkaContainer.createWithAdditionalConfiguration(brokerId, additionalKafkaConfiguration);
// startup of the Kafka container
strimziKafkaContainer.start();

```