#!/bin/bash

set -e

sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-* || true
sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-* || true

yum install sudo patch java-1.8.0-openjdk-devel wget -y
# Required by building arrow java.
wget https://downloads.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz
tar -xvf apache-maven-3.8.8-bin.tar.gz && mv apache-maven-3.8.8 /usr/lib/maven
echo "PATH=${PATH}:/usr/lib/maven/bin" >> $GITHUB_ENV

source /opt/rh/gcc-toolset-9/enable
source ./dev/build_arrow.sh
install_arrow_deps
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --enable_ep_cache=OFF --build_tests=ON \
    --build_examples=ON --build_benchmarks=ON --build_protobuf=ON

cd ./cpp/build && ctest -V
