# ==============================================================================
# 阶段 1: 编译官方最新 vllm-project/router Rust 二进制 (Standard / International)
# ==============================================================================
ARG REGISTRY_MIRROR=""
FROM ${REGISTRY_MIRROR}rustlang/rust:nightly-bookworm AS vllm-router-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 拉取最新官方 vllm-project/router 源码并编译 release 二进制
ARG GH_PROXY=""
WORKDIR /src
RUN git clone --depth 1 ${GH_PROXY}https://github.com/vllm-project/router.git /src/router
WORKDIR /src/router
RUN cargo build --release

# ==============================================================================
# 阶段 2: 编译 gpu-vllm-router (Go 调度服务 CLI)
# ==============================================================================
FROM ${REGISTRY_MIRROR}golang:1.22-bookworm AS go-builder

ARG GOPROXY=""
ENV GOPROXY=${GOPROXY}
ENV GO111MODULE=on

WORKDIR /src/gpu-vllm-router
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /src/gpu-vllm-router/bin/gpu-vllm-router ./cmd/gpu-vllm-router

# ==============================================================================
# 阶段 3: 最小化轻量生产运行镜像
# ==============================================================================
FROM ${REGISTRY_MIRROR}debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    procps \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 默认设置 UTC 时区 (可通过环境变量 TZ 自由覆盖)
ENV TZ=UTC
WORKDIR /app

# 从构建阶段复制二进制可执行文件
COPY --from=vllm-router-builder /src/router/target/release/vllm-router /usr/local/bin/vllm-router
COPY --from=go-builder /src/gpu-vllm-router/bin/gpu-vllm-router /usr/local/bin/gpu-vllm-router

RUN chmod +x /usr/local/bin/vllm-router /usr/local/bin/gpu-vllm-router

# 拷贝示例配置作为容器内默认备用配置
COPY config.example.yaml /app/config.yaml

# 创建日志持久化目录并声明数据卷
RUN mkdir -p /app/logs
VOLUME ["/app/logs"]

# 暴露对外统一服务端口 (8000: OpenAI/Swagger文档, 29000: Prometheus Metrics 专用监控)
EXPOSE 8000 29000

# 容器健康检查
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["/usr/local/bin/gpu-vllm-router"]
CMD ["-config", "/app/config.yaml"]
