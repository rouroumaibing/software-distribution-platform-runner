# software-distribution-platform-runner 运行时镜像（old images/Dockerfile 风格）。
# 由 build/runner/build.sh 在 output/runner/images/ 内使用。
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ADD software-distribution-platform-runner.tar.gz /app/
WORKDIR /app/
ENTRYPOINT ["/app/software-distribution-platform-runner"]
