# Runner image — controller-runtime manager + gateway connector.
# Build:  docker build -f deploy/Dockerfile -t localhost:5000/runner:dev .
FROM golang:1.26-alpine AS build
ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/runner ./cmd/runner

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/runner /usr/local/bin/runner
ENTRYPOINT ["runner"]
