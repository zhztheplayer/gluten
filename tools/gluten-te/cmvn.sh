#!/bin/bash

set -ex

BASEDIR=$(dirname $0)

CMVN_CMD_ARGS="$*"
MAVEN_ARGS="$CMVN_CMD_ARGS"
BASH_ARGS="mvn $MAVEN_ARGS"

$BASEDIR/cbash.sh "$BASH_ARGS"
