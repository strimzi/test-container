include ./Makefile.os
include ./Makefile.maven

RELEASE_VERSION ?= latest
PROJECT_NAME ?= strimzi-test-container

.PHONY: all
all: java_install

.PHONY: release
release: release_maven release_package

.PHONY: release_maven
release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn $(MVN_ARGS) versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn $(MVN_ARGS) versions:commit

.PHONY: release_package
release_package: java_package
	echo "Creating release archives ..."
	tar -czf target/$(PROJECT_NAME)-$(RELEASE_VERSION).tar.gz -C target $(PROJECT_NAME)-$(RELEASE_VERSION).jar
	cd target && zip $(PROJECT_NAME)-$(RELEASE_VERSION).zip $(PROJECT_NAME)-$(RELEASE_VERSION).jar

.PHONY: clean
clean: java_clean
