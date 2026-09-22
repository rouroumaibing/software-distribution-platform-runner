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

# ----- 版本渲染（2026-09-21 新增）-----
# 背景：此前 build.sh 只把 version 用在 ldflags / 镜像 tag / 文件名三处，
#       chart 内 values.yaml 的 imageAddr 与 Chart.yaml 的 version 是硬编码 v0.0.1，
#       导致发 v0.0.2 的包、chart 里仍指向 v0.0.1 镜像（交付包与镜像 tag 脱节）。
# 现在：打包前把 version 渲染进 output/charts 的副本（不动仓内源文件）。

function sed_inplace(){
    # 不用 sed -i：BSD 与 GNU 的 -i 语义不同、本机可能混装
    # （实测：GNU sed 会把 -i 后的空串当脚本、把表达式当文件名而报错），
    # 统一走「临时文件 + 覆盖」，两种平台行为一致
    local expr="$1" file="$2"
    sed "${expr}" "${file}" > "${file}.tmp" && mv "${file}.tmp" "${file}"
}

function is_semver(){
    # 严格 SemVer 2.0.0（https://semver.org/lang/zh-CN/）：MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
    # 允许 v 前缀（git tag 约定，不属于 SemVer 本体）。
    # 字面量一律用 [.] [+] 而非 \. \+，避免 BSD / GNU 的 ERE 转义差异。
    local out
    out=$(printf '%s\n' "$1" | sed -nE '/^v?(0|[1-9][0-9]*)[.](0|[1-9][0-9]*)[.](0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)([.](0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?([+]([0-9a-zA-Z-]+([.][0-9a-zA-Z-]+)*))?$/p')
    [ -n "${out}" ]
}

function render_chart_version(){
    local chartdir="${OUTPUTDIR}/charts/${component}"
    local vf="${chartdir}/values.yaml"
    local cf="${chartdir}/Chart.yaml"
    # version 会被写进 Chart.yaml（Helm 要求合法 SemVer）；非法值会让 helm lint / helm install 直接失败，
    # 故在渲染前提前失败并给出人话提示
    if ! is_semver "${version}"; then
        echo "ERROR: 版本 '${version}' 不是合法 SemVer（形如 v1.2.3 / v1.2.3-r1；参考 https://semver.org/lang/zh-CN/）" >&2
        exit 1
    fi
    local repo
    repo=$(sed -nE 's/^[[:space:]]*imageAddr:[[:space:]]*(.*):[^:]*[[:space:]]*$/\1/p' "${vf}" | head -1)
    if [ -z "${repo}" ]; then
        echo "ERROR: 未能在 ${vf} 解析 imageAddr 的仓库地址" >&2
        exit 1
    fi
    sed_inplace "s#^\([[:space:]]*imageAddr:[[:space:]]*\).*#\1${repo}:${version}#" "${vf}"
    sed_inplace "s#^version: .*#version: ${version#v}#" "${cf}"
    echo "chart rendered: imageAddr=${repo}:${version} chartVersion=${version#v}"
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
render_chart_version
build_runner
build_runner_docker_image
push_to_local_registry
charts_pack
pack
