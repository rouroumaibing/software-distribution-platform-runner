#!/bin/bash
# 构建 software-distribution-platform-runner（复刻 old/go-devops build/go-devops/build.sh 打包结构）：
#   宿主机交叉编译 -> images/ 二进制 tar.gz -> 运行时镜像 + docker save
#   charts/ chart 源目录 -> 打 tgz；最终 pack 成 <component>-<version>.tar.gz（charts + images 一起交付）
# 用法: ./build.sh [version]    默认 v0.0.1
#       ./build.sh clean        清理 output/
set -e

PROJECT_ROOT=$(cd "$(dirname "$0")"/../..; pwd)
OUTPUTDIR="${PROJECT_ROOT}/output"
component=software-distribution-platform-runner
version=${1:-"v0.0.1"}

buildDate=$(TZ=Asia/Shanghai date +%FT%T%z)

function clean(){
    rm -rf "${OUTPUTDIR}"
    echo "${component} output cleaned."
}

if [ "${1:-}" = "clean" ]; then
    clean
    exit 0
fi

function prepare_go_mod(){
    export GO111MODULE=on
    export GOPROXY=https://goproxy.cn,direct
    export GONOSUMDB='*'
    export GOSUMDB=off
    export CGO_ENABLED=0
}

function prepare_build_file(){
    mkdir -p "${OUTPUTDIR}"
    cp -rf "${PROJECT_ROOT}/build/runner/charts" "${OUTPUTDIR}/"
    cp -rf "${PROJECT_ROOT}/build/runner/images" "${OUTPUTDIR}/"
}

function build_runner(){
    mkdir -p "${OUTPUTDIR}/staging"
    pushd "${PROJECT_ROOT}" > /dev/null
    echo "== go build (linux/amd64) ${component} =="
    GOOS=linux GOARCH=amd64 go build -trimpath \
        -ldflags "-s -w -X main.buildDate=${buildDate} -X main.version=${version}" \
        -o "${OUTPUTDIR}/staging/${component}" ./cmd/runner
    popd > /dev/null
    if [ -f "${OUTPUTDIR}/staging/${component}" ]; then
        tar -zcvf "${OUTPUTDIR}/images/${component}.tar.gz" -C "${OUTPUTDIR}/staging" "${component}"
        echo "${component} build successfully."
    else
        echo "${component} build failed."
        exit 1
    fi
}

function build_runner_docker_image(){
    pushd "${OUTPUTDIR}/images" > /dev/null
    echo "== docker build ${component}:${version} =="
    docker build --network host . -t "${component}:${version}"
    docker save -o "${OUTPUTDIR}/images/${component}-${version}.tar" "${component}:${version}"
    if [ -f "${OUTPUTDIR}/images/${component}-${version}.tar" ]; then
        echo "${component} build image successfully."
    else
        echo "${component} build image failed."
        exit 1
    fi
    popd > /dev/null
}

# 可选: 有本地 registry（kind 环境 localhost:5000）时打 tag 推送，失败不阻断
function push_to_local_registry(){
    if docker tag "${component}:${version}" "localhost:5000/${component}:${version}" 2>/dev/null; then
        if docker push "localhost:5000/${component}:${version}" 2>/dev/null; then
            echo "${component} pushed to localhost:5000."
        else
            echo "WARN: push to localhost:5000 failed (no local registry?), skip."
        fi
    fi
}

function charts_pack(){
    pushd "${OUTPUTDIR}/charts" > /dev/null
    tar -zcvf "${component}-${version}.tgz" "${component}"
    if [ -f "${component}-${version}.tgz" ]; then
        echo "${component} charts pack successfully."
    else
        echo "${component} charts pack failed."
        exit 1
    fi
    popd > /dev/null
}

function pack(){
    pushd "${OUTPUTDIR}" > /dev/null
    tar -zcvf "${component}-${version}.tar.gz" charts images
    if [ -f "${component}-${version}.tar.gz" ]; then
        echo "${component} pack successfully."
    else
        echo "${component} pack failed."
        exit 1
    fi
    popd > /dev/null
}

prepare_go_mod
prepare_build_file
build_runner
build_runner_docker_image
push_to_local_registry
charts_pack
pack
