[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/test-container?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=43&branchName=main)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![GitHub release](https://img.shields.io/github/release/strimzi/test-container.svg)](https://github.com/strimzi/test-container/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/io.strimzi/strimzi-test-container)](https://search.maven.org/artifact/io.strimzi/strimzi-test-container)
[![Pull Requests](https://img.shields.io/github/issues-pr/strimzi/test-container)](https://github.com/strimzi/test-container/pulls)
[![Issues](https://img.shields.io/github/issues/strimzi/test-container)](https://github.com/strimzi/test-container/issues)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio?style=social)](https://twitter.com/strimziio)
# Test container repository

The test container repository primarily relates to developing and maintaining test container code using an [Apache Kafka®](https://kafka.apache.org) image from the [strimzi/test-container-images](https://github.com/strimzi/test-container-images) repository.
The main dependency is the Testcontainers framework, which lets you control the lifecycle of Docker containers in your tests.

> [!IMPORTANT]
> This test container repository supports **KRaft mode only**. ZooKeeper is no longer supported.  

## Why use a Test container?

Since the Docker image is a simple encapsulation of Kafka binaries, you can spin up a Kafka container rapidly.
Therefore, it is a suitable candidate for the unit or integration testing. If you need something more complex, there is a multi-node Kafka cluster implementation with only infrastructure limitations.
The most important classes are described here:
- `StrimziKafkaContainer` is a single-node instance of Kafka using the image from quay.io/strimzi-test-container/test-container with the given version.
  You can use in KRaft mode [KIP-500](https://github.com/apache/kafka/blob/trunk/config/kraft/README.md), which allows running Apache Kafka without Apache ZooKeeper.

  Additional configuration for Kafka brokers can be injected through methods such as withKafkaConfigurationMap.
  This container is a good fit for integration testing, but for more comprehensive testing, we suggest using StrimziKafkaCluster.

- `StrimziKafkaCluster` is a multi-node instance of Kafka nodes (i.e, combined-roles) and  using the latest image from quay.io/strimzi-test-container/test-container with the given version.
  It's a perfect fit for integration or system testing. 
  Additional configuration for Kafka brokers can be passed using the builder pattern.

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
// in case of 0.108.0 version
<dependency>
    <groupId>io.strimzi</groupId>
    <artifactId>strimzi-test-container</artifactId>
    <version>0.108.0</version>
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

#### iii) (Optional) Run Strimzi Kafka container on a fixed port

By default, the Kafka container will be exposed on a random host port. To expose Kafka on a fixed port:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withFixedPort(9092);

strimziKafkaContainer.start();
```

#### iv) (Optional) Run Strimzi Kafka container with a custom server.properties file

You can configure Kafka by providing a `server.properties` file:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
        .withServerProperties(MountableFile.forClasspathResource("server.properties"));
strimziKafkaContainer.start();
```

Note that configuration properties `listeners`, `advertised.listeners`, `listener.security.protocol.map`, 
`inter.broker.listener.name`, `controller.listener.names` will be overridden during container startup.
Properties configured through `withKafkaConfigurationMap` will also precede those configured in `server.properties` file.

#### v) (Optional) Run Strimzi Kafka container with a custom bootstrap servers

You can customize the bootstrap servers, thus the advertised listeners property by:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
        .withBootstrapServers(container -> String.format("SSL://%s:%s", container.getHost(), container.getMappedPort(9092)));
strimziKafkaContainer.start();
```

Note that this won't change the port exposed from the container.

#### vi) (Optional) Waiting for Kafka to be ready

Test Container can block waiting the container to be ready.
Before starting the container, use the following code configuring Test Containers to wait until Kafka becomes ready to receive connections:

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withBrokerId(1)
    .waitForRunning();

strimziKafkaContainer.start();
```

#### vii) (Optional) Specify Kafka version

Strimzi test container supported versions can be find in `src/main/java/resources/kafka_versions.json` file.

```java
StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
    .withKafkaVersion("3.8.0")
    .withBrokerId(1)
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

#### ix) (Optional) Specify a proxy container

The proxy container allows to create a TCP proxy between test code and Kafka broker.

Every Kafka broker request will pass through the proxy where you can simulate network conditions (i.e. connection cut, latency).

```java
ToxiproxyContainer proxyContainer = new ToxiproxyContainer(
        DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")
            .asCompatibleSubstituteFor("shopify/toxiproxy"));

StrimziKafkaContainer strimziKafkaContainer = new StrimziKafkaContainer()
        .withProxyContainer(proxyContainer)
        .waitForRunning();

systemUnderTest.start();

strimziKafkaContainer.getProxy().setConnectionCut(true);
```

#### x) Run a multi-node Kafka cluster

To run a multi-node Kafka cluster, you can use the StrimziKafkaCluster class with the builder pattern.

2. Specified Kafka version and additional Kafka configuration

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withKafkaVersion("3.8.0") // if not specified then latest Kafka version is selected
    .withAdditionalKafkaConfiguration(Map.of(
        "log.cleaner.enable", "false"
    ))
    .build();

kafkaCluster.start();
```

#### xi) Run a Kafka cluster with separate controller and broker roles

By default, `StrimziKafkaCluster` uses combined-role nodes where each node acts as both controller and broker. 
For more realistic production-like testing, you can configure the cluster to use dedicated controller and broker nodes:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withDedicatedRoles()
    .withNumberOfControllers(3)
    .build();

kafkaCluster.start();
```

#### xii) Log Collection for StrimziKafkaCluster

StrimziKafkaCluster supports automatic log collection from all Kafka containers. This is useful for debugging, monitoring, and analyzing the cluster behavior during tests.

##### Basic log collection with default path

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withLogCollection()  // Saves logs to target/strimzi-test-container-logs/
    .build();

kafkaCluster.start();
// ... run your tests
kafkaCluster.stop();  // Logs are automatically saved when container stops
```

##### Custom log file path

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withLogCollection("/path/to/my/logs/")  // Custom directory with trailing slash
    .build();

kafkaCluster.start();
// ... run your tests
kafkaCluster.stop();
```

##### Log collection with dedicated controller/broker roles

When using dedicated roles, logs are automatically organized by node type:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(2)
    .withDedicatedRoles()
    .withNumberOfControllers(3)
    .withLogCollection()
    .build();

kafkaCluster.start();
// ... run your tests
kafkaCluster.stop();

// Files created:
// - target/strimzi-test-container-logs/kafka-controller-0.log
// - target/strimzi-test-container-logs/kafka-controller-1.log
// - target/strimzi-test-container-logs/kafka-controller-2.log
// - target/strimzi-test-container-logs/kafka-broker-3.log
// - target/strimzi-test-container-logs/kafka-broker-4.log
```

##### Log collection with combined roles (default)

For combined-role clusters, each node gets a generic container log:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withLogCollection("my-test-logs/")
    .build();

kafkaCluster.start();
// ... run your tests
kafkaCluster.stop();

// Files created:
// - my-test-logs/kafka-container-0.log
// - my-test-logs/kafka-container-1.log
// - my-test-logs/kafka-container-2.log
```

> [!NOTE]
Log collection happens automatically when containers are stopped. 
If a path ends with "/", role-based filenames are automatically appended. 
Without the trailing slash, the exact path is used as the filename base.

#### xiii) Logging Kafka Container/Cluster Output to SLF4J

If you want to enable logging of the Kafka container’s output to SLF4J, 
you can set the environment variable `STRIMZI_TEST_CONTAINER_LOGGING_ENABLED` to true. 
By default, this feature is disabled.
This can help in debugging or monitoring the Kafka broker’s activities during tests.

### Running Mutation Testing

To run mutation testing and assess your code’s robustness against small changes, use the following Maven command:
```bash
mvn clean test-compile org.pitest:pitest-maven:mutationCoverage
```
This command will execute mutation tests based on the pitest-maven plugin and display results directly in the console.

#### Viewing Mutation Testing Results

After running the command, results will be available in an HTML report located in target/pit-reports/<timestamp>. 
Open the index.html file in a browser to view detailed information about the mutation coverage.

#### Using `@DoNotMutate` Annotation

For parts of the code primarily covered by integration tests, we can use the `@DoNotMutate` annotation. 
Applying this annotation to code ensures that mutation testing will ignore it. 
This is particularly useful for code components that are challenging to test at a unit level but well-covered in integration tests. 
Using `@DoNotMutate` helps keep mutation coverage metrics meaningful by excluding areas where mutation detection would not add value.

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
   
4. For users running tests with Podman, there is a known [issue](containers/podman#17640) that may occur:
    ```plaintext
    2024-10-31 09:48:40 ERROR [1:550] Could not start container
    java.lang.RuntimeException: com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.NoHttpResponseException: localhost:2375 failed to respond
    ```
   To resolve please update your `/etc/containers/containers.conf` with:
      ```plaintext
      [engine]
      service_timeout=0
      ```