#!/bin/bash -e

. /etc/os-release

# The default build-id used by lld is xxhash, which is 8 bytes
# long. rpm requires build-ids to be at least 16 bytes long
# (https://github.com/rpm-software-management/rpm/issues/950). We
# force using sha1 for now. That has no impact in gold and bfd since
# that is their default. We set it in here instead of configure.py to
# not slow down regular builds.

# The relocatable package includes its own dynamic linker. We don't
# know the path it will be installed to, so for now use a very long
# path so that patchelf doesn't need to edit the program headers.  The
# kernel imposes a limit of 4096 bytes including the null. The other
# constraint is that the build-id has to be in the first page, so we
# can't use all 4096 bytes for the dynamic linker.
# In here we just guess that 2000 extra / should be enough to cover
# any path we get installed to but not so large that the build-id is
# pushed to the second page.
# At the end of the build we check that the build-id is indeed in the
# first page. At install time we check that patchelf doesn't modify
# the program headers.
ORIGINAL_DYNAMIC_LINKER=$(gcc -### /dev/null -o t 2>&1 | perl -n  -e '/-dynamic-linker ([^ ]*) / && print $1')
DYNAMIC_LINKER=$(printf "%2000s$ORIGINAL_DYNAMIC_LINKER" | sed 's| |/|g')

DEFAULT_FLAGS="--enable-dpdk --cflags=-ffile-prefix-map=$PWD=. --ldflags=-Wl,--build-id=sha1,--dynamic-linker=$DYNAMIC_LINKER"

DEFAULT_MODE="release"

print_usage() {
    echo "Usage: build_reloc.sh [OPTION]..."
    echo ""
    echo "  --configure-flags FLAGS specify build flags passed to 'configure.py' (default: '$DEFAULT_FLAGS')"
    echo "  --mode MODE             specify build mode (default: '$DEFAULT_MODE')"
    echo "  --jobs JOBS             specify number of jobs"
    echo "  --clean                 clean build directory"
    echo "  --compiler PATH         C++ compiler path"
    echo "  --c-compiler PATH       C compiler path"
    exit 1
}

FLAGS="$DEFAULT_FLAGS"
MODE="$DEFAULT_MODE"
JOBS=
CLEAN=
COMPILER=
CCOMPILER=
while [ $# -gt 0 ]; do
    case "$1" in
        "--configure-flags")
            FLAGS=$2
            shift 2
            ;;
        "--mode")
            MODE=$2
            shift 2
            ;;
        "--jobs")
            JOBS="-j$2"
            shift 2
            ;;
        "--clean")
            CLEAN=yes
            shift 1
            ;;
        "--compiler")
            COMPILER=$2
            shift 2
            ;;
        "--c-compiler")
            CCOMPILER=$2
            shift 2
            ;;
        "--nodeps")
            shift 1
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}


if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of scylla dir"
    exit 1
fi

if [ "$CLEAN" = "yes" ]; then
    rm -rf build
fi

if [ -f build/$MODE/scylla-package.tar.gz ]; then
    rm build/$MODE/scylla-package.tar.gz
fi

NINJA=$(which ninja-build) &&:
if [ -z "$NINJA" ]; then
    NINJA=$(which ninja) &&:
fi
if [ -z "$NINJA" ]; then
    echo "ninja not found."
    exit 1
fi

FLAGS="$FLAGS --mode=$MODE"
if [ -n "$COMPILER" ]; then
    FLAGS="$FLAGS --compiler $COMPILER"
fi
if [ -n "$CCOMPILER" ]; then
    FLAGS="$FLAGS --c-compiler $CCOMPILER"
fi
echo "Configuring with flags: '$FLAGS' ..."
./configure.py $FLAGS
python3 -m compileall ./dist/common/scripts/ ./seastar/scripts/perftune.py ./tools/scyllatop
$NINJA $JOBS build/$MODE/scylla-package.tar.gz
BUILD_ID_END=$(readelf -SW build/$MODE/scylla | perl -ne '/.note.gnu.build-id *NOTE *[a-f,0-9]* *([a-f,0-9]*) *([a-f,0-9]*)/ && print ((hex $1) + (hex $2))')
if (($BUILD_ID_END > 4096))
then
    echo "build-id is not in the first page. It ends at offset $BUILD_ID_END"
    exit 1
fi
