FROM rockylinux:8 as base

# Install some basic utilities and build tools
RUN dnf -y install 'dnf-command(config-manager)' && \
    dnf config-manager --set-enabled devel && dnf makecache && dnf update -y ca-certificates && \
    rpm --import https://mirror.yandex.ru/rockylinux/RPM-GPG-KEY-Rocky-8 && \
    dnf -y install epel-release java-1.8.0-openjdk-devel && \
    dnf -y install git iproute net-tools openssh-server rsync sudo time vim wget unzip \
                   ant-junit autoconf bison cmake3 flex gperftools indent jq libtool gcc-c++ \
                   krb5-server krb5-workstation xerces-c-devel file netcat passwd lsof xz && \
    yum clean all

# install all software we need
RUN dnf makecache && \
    dnf -y install python39-devel && \
    dnf -y install apr-devel bzip2 bzip2-devel expat-devel libcurl-devel && \
    dnf -y install libevent-devel libuuid-devel libxml2-devel libyaml-devel libzstd-devel && \
    dnf -y install openssl-devel pam-devel readline-devel snappy-devel libuv-devel && \
    dnf -y install apache-ivy libicu perl-ExtUtils-Embed perl-Env perl-JSON && \
    dnf -y install perl-IPC-Run perl-Test-Base libxslt-devel openldap-devel libpq-devel && \
    dnf clean all

# TODO install llvm and clang packages from local repository
#RUN dnf install -y \
#    llvm-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    lvm-devel-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    llvm-libs-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    lvm-static-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    llvm-test-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    clang-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    clang-libs-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm \
#    clang-resource-filesystem-15.0.7-1.module+el8.8.0+1144+0a4e73bd.x86_64.rpm && \
#    dnf clean all

# setup ssh configuration
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys && \
    echo -e "password\npassword" | passwd 2> /dev/null && \
    ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa && \
    ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa && \
    /sbin/sshd & sleep 5 && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /root/.ssh/known_hosts && \
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers && \
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config && \
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config

# run environment for gpdb
RUN \
    echo -e '#!/bin/sh' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export JAVA_HOME=/etc/alternatives/java_sdk' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile.d/jdk_home.sh

WORKDIR /home/gpadmin

FROM base as build

COPY . gpdb_src

RUN mkdir /home/gpadmin/bin_gpdb

ENV TARGET_OS_VERSION=8
ENV TARGET_OS=rocky
ENV OUTPUT_ARTIFACT_DIR=bin_gpdb
ENV SKIP_UNITTESTS=
# To set default locale for unittests
ENV LANG=en_US.UTF-8
ENV CONFIGURE_FLAGS="--enable-debug-extensions --with-gssapi --enable-cassert --enable-debug --enable-depend"

# Compile with running mocking tests
RUN bash /home/gpadmin/gpdb_src/concourse/scripts/compile_gpdb.bash

FROM base as code
COPY . gpdb_src
RUN rm -rf gpdb_src/.git/

FROM base as test
COPY --from=code /home/gpadmin/gpdb_src gpdb_src
COPY --from=build /home/gpadmin/bin_gpdb /home/gpadmin/bin_gpdb

RUN pip3 install -r /home/gpadmin/gpdb_src/python-dependencies.txt && pip3 install behave allure-behave lxml pytest
