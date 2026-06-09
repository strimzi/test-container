include ./Makefile.os
include ./Makefile.maven

RELEASE_VERSION ?= latest
PROJECT_NAME ?= strimzi-test-container

.PHONY: all
all: java_install

.PHONY: release
release: release_maven release_json release_package

.PHONY: release_maven
release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn $(MVN_ARGS) versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn $(MVN_ARGS) versions:commit

.PHONY: release_json
release_json:
	echo "Update kafka_versions.json image tags to $(RELEASE_VERSION)"
	$(SED) -i 's#quay.io/strimzi-test-container/test-container:[a-zA-Z0-9_.-]*-kafka#quay.io/strimzi-test-container/test-container:$(RELEASE_VERSION)-kafka#g' src/main/resources/kafka_versions.json

.PHONY: release_package
release_package: java_package
	echo "Creating release archives ..."
	tar -czf target/$(PROJECT_NAME)-$(shell echo $(RELEASE_VERSION) | tr a-z A-Z).tar.gz -C target $(PROJECT_NAME)-$(shell echo $(RELEASE_VERSION) | tr a-z A-Z).jar
	cd target && zip $(PROJECT_NAME)-$(shell echo $(RELEASE_VERSION) | tr a-z A-Z).zip $(PROJECT_NAME)-$(shell echo $(RELEASE_VERSION) | tr a-z A-Z).jar

.PHONY: clean
clean: java_clean
