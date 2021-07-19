#!/bin/bash
# ----------------------------------------------
# Filename: make-CangHai.sh
# Version: 1.0 
# Date: 2021/6/18
# ----------------------------------------------
# -------------------Variable-------------------
SCRIPTE_NAME=$(readlink -f $0)
SCRIPTE_PATH=$(cd `dirname $0`; pwd)
VERSION_FUSE="CangHaiOsFuse-1.0.0"

#export GOROOT="${SCRIPTE_PATH}/go1.10"
#export PATH=$PATH:$GOROOT/bin

CangHaiGOPATH="${SCRIPTE_PATH}/gopath"
rm -rf "${CangHaiGOPATH:?}/"
mkdir -p "${CangHaiGOPATH}/CangHaiOs_src/src"
export GOPATH="${CangHaiGOPATH}/CangHaiOs_src:${GOPATH}"
export CGO_CFLAGS_ALLOW='--param.*|-Wp,-D_FORTIFY_SOURCE.*|-m64.*'
ln -s "${SCRIPTE_PATH}/CangHaiOs_git/src/code.google.com" "${CangHaiGOPATH}/CangHaiOs_src/src/code.google.com"
ln -s "${SCRIPTE_PATH}/CangHaiOs_git/src/github.com" "${CangHaiGOPATH}/CangHaiOs_src/src/github.com"

LD_FLAG_FUSE="-X code.google.com/p/weed-fs/go/util.VERSION=${VERSION_FUSE}"
rm -rf ./bin
mkdir ./bin
go build -ldflags "${LD_FLAG_FUSE}"  -o "bin/ossfuse" code.google.com/p/weed-fs/go/ossfuse
