#!/bin/bash

set -ex

BASEDIR=$(dirname $0)

$BASEDIR/../cbash.sh tools/one_step_veloxbackend.sh
