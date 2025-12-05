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
The main classes are:
- `StrimziKafkaCluster` is a Kafka cluster using the image from quay.io/strimzi-test-container/test-container with the given version.
  It supports both single-node and multi-node configurations with combined or dedicated controller/broker roles.
  It's a perfect fit for integration or system testing.
  Additional configuration for Kafka brokers can be passed using the builder pattern.

- `StrimziConnectCluster` is a Kafka Connect cluster that runs in distributed mode.
  It connects to a `StrimziKafkaCluster` and exposes a REST API for managing connectors.
  You can configure the number of workers, Kafka version, and additional Connect configuration.

## How to use the Test container?

Using Strimzi test container takes two simple steps:
1. Add the Strimzi test container as a test dependency of your project
2. Use the Strimzi test container in your tests, either
    * with the default configuration,
    * or with additional configuration you've specified

Add the Strimzi test container to your project as dependency, for example with Maven:

```
// in case of 0.113.0 version
<dependency>
    <groupId>io.strimzi</groupId>
    <artifactId>strimzi-test-container</artifactId>
    <version>0.113.0</version>
</dependency>
```

Examples:

#### i) Default configuration (single-node)

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .build();

kafkaCluster.start();
```

#### ii) (Optional) Run Strimzi Kafka cluster with additional configuration

```java
Map<String, String> additionalKafkaConfiguration = Map.of(
    "log.cleaner.enable", "false",
    "log.cleaner.backoff.ms", "1000",
    "ssl.enabled.protocols", "TLSv1",
    "log.index.interval.bytes", "2048"
);

StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .withAdditionalKafkaConfiguration(additionalKafkaConfiguration)
    .build();

kafkaCluster.start();
```

#### iii) (Optional) Run Strimzi Kafka cluster on a fixed port

By default, Kafka brokers will be exposed on random host ports. To expose brokers on fixed ports:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .withPort(9092)
    .build();

kafkaCluster.start();
```
> [!NOTE]
> When using multiple brokers, each broker gets an incrementing port starting from the specified base port.

#### iv) (Optional) Run Strimzi Kafka cluster with custom bootstrap servers

You can customize the bootstrap servers, thus the advertised listeners property by:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .withBootstrapServers(container -> String.format("SSL://%s:%s", container.getHost(), container.getMappedPort(9092)))
    .build();

kafkaCluster.start();
```

Note that this won't change the port exposed from the container.

#### v) (Optional) Specify Kafka version

Strimzi test container supported versions can be found in `src/main/java/resources/kafka_versions.json` file.

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .withKafkaVersion("4.1.1")
    .build();

kafkaCluster.start();
```
If kafka version is not set then the latest version is configured automatically. 
Latest in this scope is recently released minor version at the point of release of test-containers.

#### vi) (Optional) Specify Kafka custom image

In case you want to use your custom image (i.e., not from `src/main/java/resources/kafka_versions.json`) and
use for instance Strimzi base image you can achieve it using the builder:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .withImage("quay.io/strimzi/kafka:0.49.0-kafka-4.1.0")
    .build();

kafkaCluster.start();
```

Alternatively you can set System property `strimzi.test-container.kafka.custom.image`:

```java
// explicitly set strimzi.test-container.kafka.custom.image
System.setProperty("strimzi.test-container.kafka.custom.image", "quay.io/strimzi/kafka:0.49.0-kafka-4.1.0");

StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(1)
    .build();

kafkaCluster.start();
```

#### vii) (Optional) Specify a proxy container

The proxy container allows to create a TCP proxy between test code and Kafka nodes.

Every Kafka broker request will pass through the proxy where you can simulate network conditions (i.e. connection cut, latency, bandwidth limitations).

```java
ToxiproxyContainer proxyContainer = new ToxiproxyContainer(
        DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0")
            .asCompatibleSubstituteFor("shopify/toxiproxy"));

StrimziKafkaCluster cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
        .withNumberOfBrokers(3)
        .withProxyContainer(proxyContainer)
        .build();

cluster.start();

// Simulate network issues for a specific node (e.g., node with id = 0)
Proxy proxy = cluster.getProxyForNode(0);
proxy.setConnectionCut(true);
```

#### viii) Run a Kafka cluster with separate controller and broker roles

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

#### ix) Log Collection for StrimziKafkaCluster

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

#### x) OAuth Authentication for StrimziKafkaCluster

StrimziKafkaCluster supports OAuth authentication for secure broker communication. 
You can configure OAuth Bearer tokens or OAuth over PLAIN authentication.

##### OAuth Bearer Authentication

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withSharedNetwork()
    .withOAuthConfig(
        "demo",                        // OAuth realm name
        "kafka-broker",                // OAuth client ID
        "kafka-broker-secret",         // OAuth client secret
        "http://keycloak:8080",        // OAuth server URI
        "preferred_username"           // Username claim
    )
    .withAuthenticationType(AuthenticationType.OAUTH_BEARER)
    .build();

kafkaCluster.start();
```

##### OAuth over PLAIN Authentication

For OAuth over PLAIN, you also need to provide SASL credentials:

```java
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withSharedNetwork()
    .withOAuthConfig(
        "demo",                        // OAuth realm name
        "kafka",                       // OAuth client ID
        "kafka-secret",                // OAuth client secret
        "http://keycloak:8080",        // OAuth server URI
        "preferred_username"           // Username claim
    )
    .withAuthenticationType(AuthenticationType.OAUTH_OVER_PLAIN)
    .withSaslUsername("kafka-broker")
    .withSaslPassword("kafka-broker-secret")
    .build();

kafkaCluster.start();
```

#### xi) Logging Kafka Container/Cluster Output to SLF4J

If you want to enable logging of the Kafka container's output to SLF4J,
you can set the environment variable `STRIMZI_TEST_CONTAINER_LOGGING_ENABLED` to true.
By default, this feature is disabled.
This can help in debugging or monitoring the Kafka broker's activities during tests.

### Kafka Connect Cluster

You can run a Kafka Connect cluster alongside your Kafka cluster using `StrimziConnectCluster`.
Kafka Connect is started in distributed mode, and you can use the REST API to manage connectors.

```java
// First, create and start a Kafka cluster
StrimziKafkaCluster kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
    .withNumberOfBrokers(3)
    .withSharedNetwork()
    .build();

kafkaCluster.start();

// Then create and start the Connect cluster
StrimziConnectCluster connectCluster = new StrimziConnectCluster.StrimziConnectClusterBuilder()
    .withKafkaCluster(kafkaCluster)
    .withGroupId("my-connect-cluster")
    .build();

connectCluster.start();

// Use the REST API to manage connectors
String restEndpoint = connectCluster.getRestEndpoint();
// e.g., POST to restEndpoint + "/connectors" to create a connector
```

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