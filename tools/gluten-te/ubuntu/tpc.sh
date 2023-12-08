#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

BASEDIR=$(dirname $0)

source "$BASEDIR/build.sh"

# Non-interactive during docker run
NON_INTERACTIVE=${NON_INTERACTIVE:-$DEFAULT_NON_INTERACTIVE}

# Do not remove stopped docker container
PRESERVE_CONTAINER=${PRESERVE_CONTAINER:-$DEFAULT_PRESERVE_CONTAINER}

# Java options
EXTRA_JAVA_OPTIONS=${EXTRA_JAVA_OPTIONS:-$DEFAULT_EXTRA_JAVA_OPTIONS}

# Docker options
EXTRA_DOCKER_OPTIONS=${EXTRA_DOCKER_OPTIONS:-$DEFAULT_EXTRA_DOCKER_OPTIONS}

# Run GDB.
RUN_GDB=${RUN_GDB:-$DEFAULT_RUN_GDB}

# Run GDB server.
RUN_GDB_SERVER=${RUN_GDB_SERVER:-$DEFAULT_RUN_GDB_SERVER}

# Run JVM jdwp server.
RUN_JDWP_SERVER=${RUN_JDWP_SERVER:-$DEFAULT_RUN_JDWP_SERVER}

if [ "$RUN_GDB" == "ON" ] && [ "$RUN_GDB_SERVER" == "ON" ]
then
  echo "RUN_GDB_SERVER and RUN_GDB_SERVER can not be turned on at the same time."
  exit 1
fi


if [ "$RUN_GDB" == "ON" ]
then
  DOCKER_SELECTED_TARGET_IMAGE_TPC=${DOCKER_TARGET_IMAGE_TPC_GDB:-$DEFAULT_DOCKER_TARGET_IMAGE_TPC_GDB}
  DOCKER_BUILD_TARGET_NAME=gluten-tpc-gdb
elif [ "$RUN_GDB_SERVER" == "ON" ]
then
  DOCKER_SELECTED_TARGET_IMAGE_TPC=${DOCKER_TARGET_IMAGE_TPC_GDB_SERVER:-$DEFAULT_DOCKER_TARGET_IMAGE_TPC_GDB_SERVER}
  DOCKER_BUILD_TARGET_NAME=gluten-tpc-gdb-server
else
  DOCKER_SELECTED_TARGET_IMAGE_TPC=${DOCKER_TARGET_IMAGE_TPC:-$DEFAULT_DOCKER_TARGET_IMAGE_TPC}
  DOCKER_BUILD_TARGET_NAME=gluten-tpc
fi

DOCKER_SELECTED_TARGET_IMAGE_TPC_WITH_OS_IMAGE="$DOCKER_SELECTED_TARGET_IMAGE_TPC-$OS_IMAGE"

# GDB server bind port
GDB_SERVER_PORT=${GDB_SERVER_PORT:-$DEFAULT_GDB_SERVER_PORT}

# JVM jdwp bind port
JDWP_SERVER_PORT=${JDWP_SERVER_PORT:-$DEFAULT_JDWP_SERVER_PORT}

TPC_DOCKER_BUILD_ARGS=
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS --ulimit nofile=8192:8192"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS --build-arg DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE=$DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS --build-arg BUILD_BACKEND_TYPE=$BUILD_BACKEND_TYPE"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS -f $BASEDIR/dockerfile-tpc"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS --target $DOCKER_BUILD_TARGET_NAME"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS -t $DOCKER_SELECTED_TARGET_IMAGE_TPC_WITH_OS_IMAGE"
TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS $BASEDIR"

if [ -n "$DOCKER_CACHE_IMAGE" ]
then
  TPC_DOCKER_BUILD_ARGS="$TPC_DOCKER_BUILD_ARGS --cache-from $DOCKER_CACHE_IMAGE"
fi

TPC_DOCKER_RUN_ARGS=
if [ "$NON_INTERACTIVE" != "ON" ]
then
  TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS -it"
fi
if [ "$PRESERVE_CONTAINER" != "ON" ]
then
  CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --rm"
fi
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS --init"
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS --privileged"
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS --ulimit nofile=65536:65536"
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS --ulimit core=-1"
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS --security-opt seccomp=unconfined"
TPC_DOCKER_RUN_ARGS="$TPC_DOCKER_RUN_ARGS $EXTRA_DOCKER_OPTIONS"

TPC_CMD_ARGS="$*"

JAVA_ARGS=
if [ "$RUN_JDWP_SERVER" == "ON" ]
then
  JAVA_ARGS="$JAVA_ARGS -ea"
  JAVA_ARGS="$JAVA_ARGS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$JDWP_SERVER_PORT"
fi
JAVA_ARGS="$JAVA_ARGS $EXTRA_JAVA_OPTIONS"
JAVA_ARGS="$JAVA_ARGS -cp /opt/gluten/tools/gluten-it/package/target/lib/*"
JAVA_ARGS="$JAVA_ARGS io.glutenproject.integration.tpc.Tpc $TPC_CMD_ARGS"

BASH_ARGS=
if [ "$RUN_GDB" == "ON" ]
then
  BASH_ARGS="gdb --args java $JAVA_ARGS"
elif [ "$RUN_GDB_SERVER" == "ON" ]
then
  BASH_ARGS="$BASH_ARGS gdbserver :$GDB_SERVER_PORT java $JAVA_ARGS"
else
  BASH_ARGS="java $JAVA_ARGS"
fi

docker build $TPC_DOCKER_BUILD_ARGS
docker run $TPC_DOCKER_RUN_ARGS $DOCKER_SELECTED_TARGET_IMAGE_TPC_WITH_OS_IMAGE "$BASH_ARGS"

# EOF
