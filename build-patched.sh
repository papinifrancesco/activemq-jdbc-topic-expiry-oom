#!/usr/bin/env bash
# Builds activemq-broker and activemq-jdbc-store from the release tag of AMQ_VERSION with
# patches/jdbc-recover-expired.patch applied, into $WORK/patched/. repro.sh (PATCHED=yes)
# then puts the two jars over the ones of the official binary distribution.
#
# Needs: git, maven, a JDK in JAVA_HOME accepted by the tag's build (6.2.x: 17+, 6.3.x: 24+).
set -euo pipefail

AMQ_VERSION=${AMQ_VERSION:-6.3.2}
WORK=${WORK:-$PWD/work}
PATCH=${PATCH:-$PWD/patches/jdbc-recover-expired.patch}
SRC="$WORK/activemq-src-$AMQ_VERSION"

rm -rf "$SRC" "$WORK/patched"; mkdir -p "$WORK/patched"
git clone -q --depth 1 --branch "activemq-$AMQ_VERSION" https://github.com/apache/activemq.git "$SRC"
git -C "$SRC" apply --verbose "$PATCH"
mvn -B -q -f "$SRC/pom.xml" install -DskipTests -Drat.skip=true -Dmaven.javadoc.skip=true \
  -pl activemq-broker,activemq-jdbc-store -am
cp "$SRC/activemq-broker/target/activemq-broker-$AMQ_VERSION.jar" \
   "$SRC/activemq-jdbc-store/target/activemq-jdbc-store-$AMQ_VERSION.jar" "$WORK/patched/"
sha256sum "$WORK/patched"/*.jar
