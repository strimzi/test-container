[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# Test container repository

The test container repository primarily relates to developing and maintaining test container code using an [Apache Kafka®](https://kafka.apache.org) image from the [strimzi/test-container-images](https://github.com/strimzi/test-container-images) repository. 
The main dependency is the test container framework, which lets you control the lifecycle of the Docker container.

## Why use a Test container?

Since the Docker image is a simple encapsulation of Kafka binaries, you can spin up a Kafka container rapidly.
Therefore, it is a suitable candidate for the unit or integration testing. If you need something more complex, there is a multi-node Kafka cluster implementation with only infrastructure limitations.
The most important classes are described here::
- `StrimziKafkaContainer` is a single-node instance of Kafka using the image from quay.io/strimzi-test-container/test-container with the given version.
  You can use it in two ways:
  1. As an embedded ZooKeeper to run inside a Kafka container.
  2. As an external ZooKeeper.
  An additional configuration for Kafka brokers can be injected through the constructor.
  The first one is using an embedded ZooKeeper which will run inside Kafka container.
  Another option is to use @StrimziZookeeperContainer as an external ZooKeeper.
  An additional configuration for Kafka brokers can be injected through the constructor.
  This container is a good fit for integration testing, but for more comprehensive testing, we suggest using @StrimziKafkaCluster.
- `StrimziZookeeperContainer` is an instance of ZooKeeper encapsulated inside a Docker container using an image from quay.io/strimzi-test-container/test-container with the given version.
  It can be combined with @StrimziKafkaContainer, but we suggest using directly @StrimziKafkaCluster for more complicated testing.
- `StrimziKafkaCluster` is a multi-node instance of Kafka and ZooKeeper using the latest image from quay.io/strimzi-test-container/test-container with the given version.
  It's a perfect fit for integration or system testing. 
  We always deploy one ZooKeeper with a specified number of Kafka instances, running as a separate container inside Docker.
  The additional configuration for Kafka brokers can be passed to the constructor.

In summary, you can use `StrimziKafkaContainer` to use a Kafka cluster with a single Kafka broker.
Or you can use `StrimziKafkaCluster` for a Kafka cluster with multiple Kafka brokers.

## How to use the Test container?

Using Strimzi test container takes two simple steps:
1. Add the Strimzi test container as a test dependency of your project
2. Use the Strimzi test container in your tests, either
    * with the default configuration,
    * or with additional configuration you've specified

Add the Strimzi test container to your project as dependency, for example with Maven:

```
// in case of 0.102.0 version
<dependency>
    <groupId>io.strimzi</groupId>
    <artifactId>strimzi-test-container</artifactId>
    <version>0.102.0</version>
</dependency>
```

Examples:

#### i) default configuration 

```java
final int brokerId = 1;

StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(brokerId);
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

StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(1)
    .withKafkaConfigurationMap(additionalKafkaConfiguration);
// startup of the Kafka container
strimziKafkaContainer.start();
```

#### iii) (Optional) Run Strimzi Kafka container with KRaft (KIP-500)

[KRaft (KIP-500)](https://github.com/apache/kafka/blob/trunk/config/kraft/README.md) allows running Apache Kafka without Apache ZooKeeper. To run Kafka in KRaft mode use:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(1)
    .withKraft(true);

strimziKafkaContainer.start();
```

#### iv) (Optional) Run Strimzi Kafka container on a fixed port

By default, the Kafka container will be exposed on a random host port. To expose Kafka on a fixed port:

```java
StrimziKafkaContainer strimziKafkaContainer = strimziKafkaContainer.withFixedPort(9092);

strimziKafkaContainer.start();
```

#### v) (Optional) Run Strimzi Kafka container with a custom server.properties file

You can configure Kafka by providing a `server.properties` file:

```java
StrimziKafkaContainer strimziKafkaContainer = strimziKafkaContainer
        .withServerProperties(MountableFile.forClasspathResource("server.properties"));
strimziKafkaContainer.start();
```

Note that configuration properties `listeners`, `advertised.listeners`, `listener.security.protocol.map`, 
`inter.broker.listener.name`, `controller.listener.names`, `zookeeper.connect` will be overridden during container startup.
Properties configured through `withKafkaConfigurationMap` will also precede those configured in `server.properties` file.

#### vi) (Optional) Run Strimzi Kafka container with a custom bootstrap servers

You can customize the bootstrap servers, thus the advertised listeners property by:

```java
StrimziKafkaContainer strimziKafkaContainer = strimziKafkaContainer
        .withBootstrapServers(container -> String.format("SSL://%s:%s", container.getHost(), container.getMappedPort(9092)));
strimziKafkaContainer.start();
```

Note that this won't change the port exposed from the container.

#### vii) (Optional) Waiting for Kafka to be ready

Test Container can block waiting the container to be ready.
Before starting the container, use the following code configuring Test Containers to wait until Kafka becomes ready to receive connections:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(1)
    .withKraft(true)
    .waitForRunning();

strimziKafkaContainer.start();
```

#### vii) (Optional) Specify Kafka version

Strimzi test container supported versions can be find in `src/main/java/resources/kafka_versions.json` file.

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withKafkaVersion("2.8.0")
    .withBrokerId(1)
    .withKraft(true)
    .waitForRunning();

strimziKafkaContainer.start();
```
If kafka version is not set then the latest version is configured automatically. Latest in this scope is recently 
released minor version at the point of release of test-containers.

#### viii) (Optional) Specify Kafka custom image

In case you want to use your custom image (i.e., not from `src/main/java/resources/kafka_versions.json`) and 
use for instance Strimzi base image you can achieve it by passing the image name to the constructor:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer("quay.io/strimzi/kafka:0.27.1-kafka-3.0.0")
    .withBrokerId(1)
    .waitForRunning();

strimziKafkaContainer.start();
```

Alternatively you can set System property `strimzi.test-container.kafka.custom.image`:

```java
// explicitly set strimzi.test-container.kafka.custom.image
System.setProperty("strimzi.test-container.kafka.custom.image", "quay.io/strimzi/kafka:0.27.1-kafka-3.0.0");

StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(1)
    .waitForRunning();

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

3. To use `podman` instead of `docker` you'll need to enable the podman socket and export a couple of environment variables:
    ```bash
    systemctl --user enable podman.socket --now
    export DOCKER_HOST=unix:///run/user/${UID}/podman/podman.sock
    export TESTCONTAINERS_RYUK_DISABLED=true
    ```