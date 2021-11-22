[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# Test container repository

The test container repository primarily relates to developing and maintaining test container code using `Kafka` image from the [strimzi/test-container-images](https://github.com/strimzi/test-container-images) repository. 
The main dependency is the test container framework, which lets you control the lifecycle of the Docker container.

## Why use a Test container?

Since the Docker image is a simple encapsulation of Kafka binaries, you can spin up a Kafka container rapidly.
Therefore, it is a suitable candidate for the unit or integration testing. If you need something more complex, there is a multi-node Kafka cluster implementation with only infrastructure limitations.
The most important classes are described here::
- `StrimziKafkaContainer` is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the given version.
  You can use it in two ways:
  1. As an embedded ZooKeeper to run inside a Kafka container.
  2. As an external ZooKeeper.
  An additional configuration for Kafka brokers can be injected through the constructor.
  The first one is using an embedded ZooKeeper which will run inside Kafka container.
  Another option is to use @StrimziZookeeperContainer as an external ZooKeeper.
  An additional configuration for Kafka brokers can be injected through the constructor.
  This container is a good fit for integration testing, but for more comprehensive testing, we suggest using @StrimziKafkaCluster.
- `StrimziZookeeperContainer` is an instance of ZooKeeper encapsulated inside a Docker container using an image from quay.io/strimzi/kafka with the given version.
  It can be combined with @StrimziKafkaContainer, but we suggest using directly @StrimziKafkaCluster for more complicated testing.
- `StrimziKafkaCluster` is a multi-node instance of Kafka and ZooKeeper using the latest image from quay.io/strimzi/kafka with the given version.
  It's a perfect fit for integration or system testing. 
  We always deploy one ZooKeeper with a specified number of Kafka instances, running as a separate container inside Docker.
  The additional configuration for Kafka brokers can be passed to the constructor.

In summary, you can use `StrimziKafkaContainer` to use a Kafka cluster with a single Kafka broker.
Or you can use `StrimziKafkaCluster` for a Kafka cluster with multiple Kafka brokers.

## How to use the Test container?

Using a given test container takes three simple steps:
1. Specify two environment variables
2. Add the Strimzi test container as a Maven dependency
3. Run Strimzi test container
    1. default configuration
    2. (optional) run Strimzi test container with additional configuration

First, you need to specify two essential environment variables:

- `STRIMZI_TEST_CONTAINER_KAFKA_VERSION` - specification of the Kafka version to run with the given container. For example, `3.0.0`.
- `STRIMZI_TEST_CONTAINER_IMAGE_VERSION` - specification of the Strimzi test container image version that was built in the `test-container-images` repositories. For example, `0.1.0`.

Add the Strimzi test container to the project as a Maven dependency:

```
// in case of 0.26.0 version
<dependency>
    <groupId>io.strimzi</groupId>
    <artifactId>strimzi-test-container</artifactId>
    <version>0.26.0</version>
</dependency>
```

Examples:

#### i) default configuration

```java
final int brokerId = 1;

StrimziKafkaContainer strimziKafkaContainer = StrimziKafkaContainer.create(brokerId);
// startup of the Kafka container
strimziKafkaContainer.start();
```
#### ii) (Optional) Run Strimzi test container with additional configuration

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

#### iii) (Optional) Run Strimzi Kafka container with KRaft (KIP-500)

[KRaft (KIP-500)](https://github.com/apache/kafka/blob/trunk/config/kraft/README.md) allows running Apache Kafka without Apache ZooKeeper. To run Kafka in KRaft mode use:

```java
StrimziKafkaContainer strimziKafkaContainer = StrimziKafkaContainer.createWithKraft(1);

strimziKafkaContainer.start();
```

#### iv) Waiting for Kafka to be ready

Test Container can block waiting the container to be ready.
Before starting the container, use the following code configuring Test Containers to wait until Kafka becomes ready to receive connections:

```java
StrimziKafkaContainer strimziKafkaContainer = strimziKafkaContainer.waitForRunning();
strimziKafkaContainer.start();
```

### Additional tips

1. In case you are using `Azure pipelines` Ryuk needs to be turned off, since Azure does not allow starting privileged containers.
   Disable Ryuk by setting an environment variable::
    - `TESTCONTAINERS_RYUK_DISABLED` - TRUE
    
2. By default, `TestContainers` performs a series of start-up checks to ensure that the environment is configured correctly.
   It takes a couple of seconds, but if you want to speed up your tests, you can disable the checks once you have everything configured.
   Disable start-up checks by setting an environment variable:
   - `TESTCONTAINERS_CHECKS_DISABLE` - TRUE