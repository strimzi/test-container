RELEASE_VERSION ?= latest

include ./Makefile.os

release: release_maven

next_version:
	mvn versions:set -DnewVersion=$(shell echo $(NEXT_VERSION) | tr a-z A-Z)
	mvn versions:commit

release_maven:
	echo "Update pom versions to: $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit