#!/bin/sh -e
export RPMBUILD=`pwd`/build/rpmbuild

do_install()
{
    pkg=$1
    name=${pkg/%.rpm/}
    if ! rpm -qs $name  >/dev/null 2>&1; then
	sudo yum install -y $RPMBUILD/RPMS/x86_64/$pkg || sudo yum install -y $RPMBUILD/RPMS/noarch/$pkg
    fi
    echo Install $name done
}

sudo yum install -y wget yum-utils rpm-build rpmdevtools gcc gcc-c++ make patch
mkdir -p build/srpms
cd build/srpms

if [ ! -f boost-1.57.0-6.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/b/boost-1.57.0-6.fc22.src.rpm
fi

if [ ! -f ninja-build-1.5.3-2.fc22.src.rpm ]; then
    wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/n/ninja-build-1.5.3-2.fc22.src.rpm
fi

if [ ! -f ragel-6.8-3.fc22.src.rpm ]; then
   wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/r/ragel-6.8-3.fc22.src.rpm
fi

if [ ! -f re2c-0.13.5-9.fc22.src.rpm ]; then
   wget http://download.fedoraproject.org/pub/fedora/linux/releases/22/Everything/source/SRPMS/r/re2c-0.13.5-9.fc22.src.rpm
fi

cd -

sudo yum install -y epel-release
sudo yum install -y cryptopp cryptopp-devel jsoncpp jsoncpp-devel lz4 lz4-devel yaml-cpp yaml-cpp-devel thrift thrift-devel scons gtest gtest-devel python34
sudo ln -sf /usr/bin/python3.4 /usr/bin/python3

sudo yum install -y scl-utils
if ! rpm  -qs rhscl-devtoolset-3-epel-7-x86_64-1-2.noarch; then
    sudo yum install -y https://www.softwarecollections.org/en/scls/rhscl/devtoolset-3/epel-7-x86_64/download/rhscl-devtoolset-3-epel-7-x86_64.noarch.rpm
fi
sudo yum install -y devtoolset-3-gcc-c++

sudo yum install -y python-devel libicu-devel openmpi-devel mpich-devel libstdc++-devel bzip2-devel zlib-devel
if [ ! -f $RPMBUILD/RPMS/x86_64/boost-1.57.0-6.el7.centos.x86_64.rpm ]; then
    rpmbuild --define "_topdir $RPMBUILD" --without python3 --rebuild build/srpms/boost-1.57.0-6.fc22.src.rpm
fi
for i in `ls $RPMBUILD/RPMS/x86_64/boost*|grep -v debuginfo`;do
    do_install `basename $i`
done

if [ ! -f $RPMBUILD/RPMS/x86_64/re2c-0.13.5-9.el7.centos.x86_64.rpm ]; then
    rpmbuild --define "_topdir $RPMBUILD" --rebuild build/srpms/re2c-0.13.5-9.fc22.src.rpm
fi
do_install re2c-0.13.5-9.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/ninja-build-1.5.3-2.el7.centos.x86_64.rpm ]; then
   rpm --define "_topdir $RPMBUILD" -ivh build/srpms/ninja-build-1.5.3-2.fc22.src.rpm
   patch $RPMBUILD/SPECS/ninja-build.spec < dist/redhat/centos_dep/ninja-build.diff
   rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/ninja-build.spec
fi
do_install ninja-build-1.5.3-2.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/ragel-6.8-3.el7.centos.x86_64.rpm ]; then
    sudo yum install -y gcc-objc
    rpm --define "_topdir $RPMBUILD" -ivh build/srpms/ragel-6.8-3.fc22.src.rpm
    patch $RPMBUILD/SPECS/ragel.spec < dist/redhat/centos_dep/ragel.diff
    rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/ragel.spec
fi
do_install ragel-6.8-3.el7.centos.x86_64.rpm

if [ ! -f $RPMBUILD/RPMS/noarch/antlr3-tool-3.5.2-1.el7.centos.noarch.rpm ]; then
   mkdir build/antlr3-tool-3.5.2
   cp dist/redhat/centos_dep/antlr3 build/antlr3-tool-3.5.2
   cd build/antlr3-tool-3.5.2
   wget http://www.antlr3.org/download/antlr-3.5.2-complete-no-st3.jar
   cd -
   cd build
   tar cJpf $RPMBUILD/SOURCES/antlr3-tool-3.5.2.tar.xz antlr3-tool-3.5.2
   cd -
   rpmbuild --define "_topdir $RPMBUILD" -ba dist/redhat/centos_dep/antlr3-tool.spec
fi
do_install antlr3-tool-3.5.2-1.el7.centos.noarch.rpm

if [ ! -f $RPMBUILD/RPMS/x86_64/antlr3-C++-devel-3.5.2-1.el7.centos.x86_64.rpm ];then
   wget -O build/3.5.2.tar.gz https://github.com/antlr/antlr3/archive/3.5.2.tar.gz
   mv build/3.5.2.tar.gz $RPMBUILD/SOURCES
   rpmbuild --define "_topdir $RPMBUILD" -ba dist/redhat/centos_dep/antlr3-C++-devel.spec
fi
do_install antlr3-C++-devel-3.5.2-1.el7.centos.x86_64.rpm
