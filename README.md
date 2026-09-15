# GPUStack 与 vLLM-Router 动态实例调度与网关工具 (`gpu-vllm-router-cli`)

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![vLLM Router](https://img.shields.io/badge/vLLM--Router-vllm--project-green)](https://github.com/vllm-project/router)
[![GPUStack](https://img.shields.io/badge/GPUStack-Compatible-orange)](https://github.com/gpustack/gpustack)
[![Concurrency](https://img.shields.io/badge/High--Concurrency-30M_QPS_FastPath-brightgreen)](#)
[![Metrics](https://img.shields.io/badge/Prometheus-Port_29000-blueviolet)](#十prometheus-监控体系与核心指标规范port-29000)
[![Auto-Heal](https://img.shields.io/badge/Auto--Heal-OOM_Mitigation-red)](#九无人值守自动化故障自愈auto-heal机制)

`gpu-vllm-router-cli` 是一个专为 **[GPUStack](https://github.com/gpustack/gpustack)** AI 集群与 **[vLLM-Router](https://github.com/vllm-project/router)**（vLLM 官方高性能请求路由器）打造的纯 Go 命令行工具与轻量后端桥接系统。

本项目为**零前端依赖的纯后端 CLI 版本**（不含 Node.js / React 前端页面，无多余构建负担，体积小巧、编译极速），核心聚焦于：
1. **生成后端启动指令 (`-mode cmd`)**：一键发现 GPUStack 实例并生成官方 vLLM-Router 的标准 Bash、PowerShell、Docker 启动指令。
2. **内置纯 Go 高可用网关 (`-mode proxy`)**：零外部依赖，直接作为兼容 OpenAI API 的反向代理网关。
3. **官方路由器守护与热重载 (`-mode run`)**：双进程蓝绿零停机滚动热重载。
4. **独立 Prometheus 监控体系**：专用 29000 端口导出官方 `vllm_router_` 标准指标群。
5. **深度就绪探测与故障自愈**：优先校验 `/v1/models` 防模型加载中击穿，自动捕获 CUDA OOM 并触发冷却重启与现场审计归档。

---

## 目录

- [一、核心架构与系统拓扑](#一核心架构与系统拓扑)
- [二、主要功能特性](#二主要功能特性)
- [三、六大负载均衡策略详解](#三六大负载均衡策略详解)
- [四、快速上手指南](#四快速上手指南)
  - [1. 列出集群模型](#1-列出集群模型)
  - [2. 模式一：生成启动命令 (`-mode cmd`)](#2-模式一生成启动命令--mode-cmd)
  - [3. 模式二：内置 Go 高可用网关 (`-mode proxy` 全集群多模型负载)](#3-模式二内置-go-高可用网关--mode-proxy-全集群多模型负载)
  - [4. 模式三：官方路由器守护与零停机重载 (`-mode run`)](#4-模式三官方路由器守护与零停机重载--mode-run)
- [五、全集群多模型动态路由机制](#五全集群多模型动态路由机制)
- [六、零停机热重载（Zero-Downtime）原理解析](#六零停机热重载zero-downtime原理解析)
- [七、熔断器（Circuit Breaker）与透明故障转移（Failover Retry）](#七熔断器circuit-breaker与透明故障转移failover-retry)
- [八、大模型权重加载与深度就绪探测（Readiness Probing）](#八大模型权重加载与深度就绪探测readiness-probing)
- [九、无人值守自动化故障自愈（Auto-Heal）机制](#九无人值守自动化故障自愈auto-heal机制)
- [十、Prometheus 监控体系与核心指标规范（Port 29000）](#十prometheus-监控体系与核心指标规范port-29000)
- [十一、命令行参数与环境变量全集](#十一命令行参数与环境变量全集)
- [十二、配置文件使用 (`config.yaml`)](#十二配置文件使用-configyaml)
- [十三、管理、监控与 API 文档接口](#十三管理监控与-api-文档接口)
- [十四、生产部署与运维建议](#十四生产部署与运维建议)
- [十五、编译、测试与性能基准](#十五编译测试与性能基准)
- [十六、Docker 与 Docker-Compose 容器化部署](#十六docker-与-docker-compose-容器化部署)

---

## 一、核心架构与系统拓扑

系统整体采用**分层解耦、双模调度、自愈防护**的企业级高并发架构设计：

```mermaid
flowchart TD
    subgraph ClientAndObservability ["1. 客户端访问与监控接入层"]
        ClientApp["业务应用 / 前端 WebUI / LangChain<br/>(统一访问端口 :8000)"]
        PrometheusNode["Prometheus 监控服务 / Grafana 看板<br/>(独立拉取端口 :29000 /metrics)"]
    end

    subgraph RouterCore ["2. gpu-vllm-router 智能调度与调度中枢"]
        direction TB
        
        subgraph Gateways ["接入网关与传输加速层"]
            FrontGW["HTTP/OpenAI 业务网关 (:8000)<br/>• 动态模型路由分发 (/v1/chat/completions)<br/>• Swagger UI / ReDoc 调试文档 (/docs)<br/>• 拓扑与配置接口 (/api/topology, /api/config)"]
            MetricsGW["Prometheus 独立监控服务 (:29000)<br/>• 暴露 vllm_router 规范官方指标库"]
            ConnPool["高并发长连接池 (NewOptimizedTransport)<br/>• MaxIdleConnsPerHost: 2048<br/>• HTTP/2 多路复用 & 禁用流式反压缩<br/>• req.GetBody 请求体回滚透明重试"]
            BodyCache["请求体单次上下文快照 (getOrReadRequestBody)<br/>• 7.2x 读取提速 / 3.9x 堆内存开销降低"]
        end

        subgraph DualEngines ["双模执行与负载均衡引擎"]
            subgraph ModeRun ["模式 A: mode=run (官方 vllm-router 进程监督池)"]
                Supervisor["Supervisor 进程调度大脑"]
                CandidateProc["蓝绿候选进程 (端口: 18002)<br/>• 权重就绪探测通过后原子切流"]
                ActiveProc["当前主力进程 (端口: 18001)<br/>• 60s 优雅排空 (Drain Timeout) 后平滑退出"]
            end

            subgraph ModeProxy ["模式 B: mode=proxy (纯 Go 内置高性能网关)"]
                GoProxy["纯 Go 反向代理引擎 (零外部环境依赖)"]
                Balancers["6 大负载均衡算法池<br/>• 一致性哈希 (ConsistentHash)<br/>• 前缀缓存感知 (CacheAware)<br/>• 负载感知 P2C / 轮询 / 随机 / Rendezvous"]
            end
        end

        subgraph FaultTolerance ["智能容灾、就绪探测与自愈引擎"]
            FastBreaker["三态断路器 (Circuit Breaker)<br/>• RWMutex Fast-Path (29.7M QPS 无锁检查)<br/>• 毫秒级故障隔离 & 透明故障转移 (Failover Retry)"]
            ReadinessProbe["深度就绪探测器 (Readiness Prober)<br/>• 优先校验 /v1/models (权重加载深度校验)<br/>• 杜绝 9→17 扩容加载中虚假健康 (Health!=Active)"]
            AutoHealer["无人值守故障自愈器 (Auto-Healer)<br/>• 智能捕捉 CUDA OOM / NCCL 通信异常<br/>• 冷启动保护 (Booting Guard) & 防抖动重启冷却<br/>• 现场取证审计落盘 (logs/crashes/<model>/...)"]
        end

        TopologyWatcher["后台拓扑感知同步器 (Periodic Watcher 10s)"]
        GPUStackClient["GPUStack API 客户端 (自动 Token 认证 / API 通信)"]
    end

    subgraph GPUStackInfra ["3. GPUStack AI 集群与计算基础设施 (http://<GPUSTACK>:8200)"]
        GS_ControlPlane["GPUStack Control Plane (控制面 API)"]
        subgraph GPUWorkers ["分布式 GPU 推理物理节点 / 实例"]
            W1["Worker 1: worker-node-1<br/>192.168.1.10:40039 (vLLM / SGLang)"]
            W2["Worker 2: worker-node-2<br/>192.168.1.11:40039 (vLLM / SGLang)"]
            WN["Worker N: worker-node-n<br/>192.168.1.xx:40039 (动态扩容实例)"]
        end
    end

    ClientApp -->|"HTTP/1.1 & HTTP/2 请求"| FrontGW
    PrometheusNode -->|"拉取指标 :29000/metrics"| MetricsGW

    FrontGW --> BodyCache
    BodyCache --> ConnPool

    ConnPool -->|"mode=run 内部代理"| Supervisor
    Supervisor --> ActiveProc
    Supervisor -.->|"实例变动时无损切流"| CandidateProc

    ActiveProc -->|"长连接复用"| W1
    ActiveProc -->|"长连接复用"| W2
    CandidateProc -->|"扩容流量对接"| WN

    ConnPool -->|"mode=proxy 直连分发"| GoProxy
    GoProxy --> Balancers
    Balancers --> FastBreaker
    FastBreaker --> W1
    FastBreaker --> W2
    FastBreaker --> WN

    ReadinessProbe -->|"就绪深度嗅探 /v1/models"| WN
    AutoHealer -->|"异常重启命令"| GS_ControlPlane
    TopologyWatcher --> GPUStackClient
    GPUStackClient -->|"轮询集群实例拓扑"| GS_ControlPlane
    TopologyWatcher -->|"触发蓝绿重载"| Supervisor
    TopologyWatcher -->|"更新健康节点环"| Balancers
```

---

## 二、主要功能特性

1. **全集群多模型自动纳管与智能路由（无需指定模型名）**
   - 支持一键同步 GPUStack 集群上的**全部模型**及其对应运行实例，自动构建多模型路由表。
   - 对外统一暴露 OpenAI 兼容网关，请求到达时自动解析请求体中的 `"model"` 参数，分发至对应模型的负载均衡池。
   - 自动聚合全集群就绪模型的 `GET /v1/models` 列表，与各大前端/框架（如 NextChat, OpenWebUI, LangChain）完美对接。

2. **生产级超高并发与低延迟架构 (High-Concurrency Architecture)**
   - **29.7M QPS 熔断检查**：采用 RWMutex 读锁快速路径，99.99% 正常流量实现 50ns 极速无锁放行，0 内存分配。
   - **微秒级请求体单次上下文快照**：单次解析并利用 context 缓存，彻底根除模型提取与会话计算中的 2~4 次冗余 `io.ReadAll`，耗时由 111.5 µs 降低至 15.5 µs（**7.2 倍提速**），堆内存分配降低 **3.9 倍**。
   - **大容量长连接池常驻复用**：内置 `NewOptimizedTransport`，提供单主机 2048+ 空闲长连接槽位与 `req.GetBody` 自动回滚重试机制，彻底消除端口耗尽与 `TIME_WAIT` 堆积。

3. **专设 Prometheus 29000 端口与标准 vLLM 监控体系**
   - 采用标准双网络端口架构，业务通信（:8000）与监控采集（:29000）完全物理隔离，互不阻塞。
   - 完整暴露 `vllm_router_running_requests`、`vllm_router_processed_requests_total`、`vllm_router_cb_outcomes_total` 等官方指标群。

4. **大模型权重深度就绪探测 (Readiness Probing)**
   - 优先通过 `/v1/models` 检测模型是否已真正载入显存并就绪（返回非空模型列表）；
   - 完美解决 GPUStack 实例扩容（如 9→17 实例）时因模型加载中导致的 `health: 17, active: 9` 状态撕裂与候选路由器过早超时失败问题。

5. **无人值守自动化故障自愈 (Auto-Heal System)**
   - 后台主动感知断路器 OPEN 状态，秒级拉取容器日志。
   - 智能识别 `CUDA out of memory`、`NCCL error` 等致命故障，调用 GPUStack API 触发实例平滑重启。
   - 具备冷启动保护（Booting Guard）、防频跳重启冷却期与现场日志落盘审计（`logs/crashes`）。

6. **支持官方全部 6 种负载均衡策略**
   - 覆盖一致性哈希、Prefix-Caching 缓存感知、轮询、P2C、随机分发等算法，兼顾高并发吞吐与大模型 KV-Cache 上下文命中率。
   - 多模型模式下，各个模型拥有**完全独立的负载均衡器**，互不干扰。

7. **双重保障运行形态**
   - **支持官方 Rust 二进制 (`-mode run`)**：完整兼容 `vllm-project/router` 官方参数体系，蓝绿双进程无感切换与 60s 优雅排空。
   - **支持零依赖纯 Go 网关 (`-mode proxy`)**：本地未安装 Rust/Cargo 也能立即运行，毫秒级流式转发与状态管理。

8. **细粒度日志分级与模型日志隔离**
   - 支持 `debug`、`info`、`warn`、`error` 四级日志动态过滤。常规高频路由日志降级为 `debug`，杜绝日志全局互斥锁瓶颈。
   - 支持模型级别日志隔离归档（`log_dir`），方便精细化运维排查。

---

## 三、六大负载均衡策略详解

可以通过命令行随时查看受支持的策略：
```powershell
.\gpu-vllm-router.exe -list-policies
```

| 策略标识 (`-policy`) | 中文名称 | 会话亲和性 | 负载感知 | 适用业务场景与核心收益 |
| :--- | :--- | :---: | :---: | :--- |
| **`consistent_hash`**<br>*(推荐默认)* | **一致性哈希** | **是** | 否 | **多轮对话系统、智能客服、Agent 场景**。<br>根据会话标识将同一用户的连续交互精准路由到同一 Worker 节点，最大化复用 **KV Cache**，显著降低首字延迟（TTFT）。 |
| **`cache_aware`** | **前缀缓存感知** | **是** (Cache) | **是** | **企业知识库（RAG）、公用长 Prompt 场景**。<br>根据 Prompt 共享前缀深度优化缓存重用，兼顾各 Worker 的负载倾斜度。 |
| **`round_robin`** | **平滑轮询** | 否 | 否 | **单轮问答、基准评测、无状态请求**。<br>在各健康节点间严格均等交替分发流量。 |
| **`power_of_two`**<br>*(P2C)* | **两选一负载均衡** | 否 | **是** | **请求耗时差异悬殊、长短文本混杂场景**。<br>每次随机抽取两个 Worker，优先分发给当前活跃在途连接数（Active Connections）较少的节点，避免单点过载。 |
| **`random`** | **均匀随机** | 否 | 否 | **超大节点集群、简单微服务部署**。<br>纯随机轮换，分发开销极低。 |
| **`rendezvous_hash`** | **最高随机权重哈希** | **是** | 否 | **节点频繁动态增删集群**。<br>相比一致性哈希在节点下线时数据迁移更加平滑均匀。 |

### 会话保持 Key 优先级（针对 `consistent_hash`）
为了让多轮对话命中同一个实例，工具支持按以下优先级自动提取会话指纹：
1. HTTP 请求头：`X-Session-ID`（推荐，性能最高）
2. HTTP 请求头：`X-User-ID`
3. HTTP 请求头：`X-Tenant-ID`
4. HTTP 请求头：`X-Request-ID`
5. 请求 Body JSON 字段：`session_params.session_id`
6. 请求 Body JSON 字段：`user`（OpenAI 标准字段）
7. 请求 Body JSON 字段：`session_id`
8. 回退机制：客户端远端 IP（`RemoteAddr`）

---

## 四、快速上手指南

### 1. 列出集群模型
```powershell
# 查看 GPUStack 集群当前已上线或已配置的模型
.\gpu-vllm-router.exe -gpustack-url "http://<GPUSTACK_HOST>:8200" -username admin -password "your_password" -list-models
```
*输出将展示当前所有模型 ID、模型名称、副本总数与健康就绪副本数。*

---

#### 2. 模式一：生成启动命令 (`-mode cmd`)

#### 方式 A：全集群多模型自动生成（默认，不指定 `-model`）
```powershell
.\gpu-vllm-router.exe -mode cmd
```
程序将自动遍历 GPUStack 上的所有模型，输出每个模型的健康实例列表及专属启动命令。

#### 方式 B：指定特定模型
```powershell
.\gpu-vllm-router.exe -model DeepSeek-V4-Flash-0731-w8a8 -policy consistent_hash -mode cmd
```

**输出示例：**
```text
================= GPUStack 实例服务发现结果 =================
模型名称: DeepSeek-V4-Flash-0731-w8a8 (ID: 13, 后端: vLLM)
健康工作点数量: 1
-------------------------------------------------------------------
实例名称               | Worker 节点       | IP 地址            | 端口     | 工作点接入 URL
-------------------------------------------------------------------
DeepSeek-V4-Flash-0731-w8a8-WCQIP | worker-node-1   | 192.168.1.10     | 40039  | http://192.168.1.10:40039
===================================================================

>>> 1. Linux / macOS (Bash) 启动命令:
vllm-router \
    --host 0.0.0.0 \
    --port 8000 \
    --policy consistent_hash \
    --log-level info \
    --worker-urls http://192.168.1.10:40039

>>> 2. Windows (PowerShell) 启动命令:
vllm-router `
    --host 0.0.0.0 `
    --port 8000 `
    --policy consistent_hash `
    --log-level info `
    --worker-urls http://192.168.1.10:40039

>>> 3. Docker 容器化启动命令:
docker run --rm -it --network host -p 8000:8000 vllm/vllm-router:latest --host 0.0.0.0 --port 8000 --policy consistent_hash --log-level info --worker-urls http://192.168.1.10:40039
```

---

### 3. 模式二：内置 Go 高可用网关 (`-mode proxy` 全集群多模型负载)
无需在机器上编译或安装 Rust/Cargo，Go 程序自身直接作为兼容 OpenAI API 的反向代理网关，**原生支持全集群多模型自动聚合与按模型动态路由**。

```powershell
# 启动内置代理网关（不传 -model，默认同步全集群所有模型）
.\gpu-vllm-router.exe -mode proxy -port 8000 -policy consistent_hash

# 也可以锁定特定单模型：
# .\gpu-vllm-router.exe -model DeepSeek-V4-Flash-0731-w8a8 -mode proxy -port 8000
```

#### 测试与验证调用：
- **查看集群所有模型的负载状态与节点健康度**：
  ```bash
  curl http://localhost:8000/admin/stats
  ```
  *返回集群全景视图：*
  ```json
  {
    "cluster_mode": true,
    "model_count": 1,
    "models": {
      "DeepSeek-V4-Flash-0731-w8a8": {
        "backend_count": 1,
        "backends": [
          {
            "url": "http://192.168.1.10:40039",
            "active_conns": 0,
            "healthy": true
          }
        ],
        "model": "DeepSeek-V4-Flash-0731-w8a8",
        "policy": "consistent_hash"
      }
    }
  }
  ```

- **获取 OpenAI 格式模型列表（自动聚合集群所有就绪模型）**：
  ```bash
  curl http://localhost:8000/v1/models
  ```
  *返回：*
  ```json
  {
    "object": "list",
    "data": [
      {
        "id": "DeepSeek-V4-Flash-0731-w8a8",
        "object": "model",
        "owned_by": "gpustack"
      }
    ]
  }
  ```

- **发起模型推理请求（自动路由到对应模型的负载均衡池）**：
  ```bash
  curl -X POST http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -H "X-Session-ID: session_12345" \
    -d '{
      "model": "DeepSeek-V4-Flash-0731-w8a8",
      "messages": [{"role": "user", "content": "你好"}],
      "stream": true
    }'
  ```

---

### 4. 模式三：官方路由器守护与零停机重载 (`-mode run` 生产推荐)
若服务器已编译安装好官方 `vllm-router` 二进制可执行文件（或使用容器运行），Supervisor 将作为进程管理调度大脑：

#### 方式 A：全集群多模型多进程路由器池（默认，无需指定 `-model`）
```powershell
.\gpu-vllm-router.exe `
    -mode run `
    -router-bin vllm-router `
    -port 8000 `
    -watch-interval 10s `
    -zero-downtime=true `
    -drain-timeout=60s
```
**工作机制**：
- Supervisor 自动发现 GPUStack 中的所有模型，为**每一个模型**分别分配一个内部端口并启动独立的官方 `vllm-router` 进程（如 DeepSeek 运行在 18001，Qwen 运行在 18002）。
- 前置 8000 端口网关对外统一提供服务，自动嗅探请求体中的 `"model"` 参数，零延迟精准转发给对应模型的 `vllm-router`。
- 当某个模型扩容/缩容时，Supervisor 仅对该模型的 `vllm-router` 进程执行**蓝绿零停机热重载**，其他模型 100% 保持不受影响！
- 访问 `GET http://localhost:8000/admin/supervisor` 可查看所有纳管模型的内部端口与运行拓扑。

#### 方式 B：单模型精准守护模式（指定 `-model`）
```powershell
.\gpu-vllm-router.exe `
    -model DeepSeek-V4-Flash-0731-w8a8 `
    -policy consistent_hash `
    -mode run `
    -router-bin vllm-router `
    -port 8000 `
    -watch-interval 10s `
    -zero-downtime=true `
    -drain-timeout=60s
```
只为该目标模型拉起官方 `vllm-router` 并进行单模型蓝绿滚动热重载。

---

## 五、全集群多模型动态路由机制

传统模式下每个路由只绑定一个模型，而在现代 AI 集群中，往往部署了多种尺寸和类型的模型（如通用对话模型、代码大模型、推理思考模型等）。

`gpu-vllm-router` 的全集群多模型路由机制工作流如下：

```mermaid
flowchart TD
    ClientReq["客户端请求<br/>POST /v1/chat/completions<br/>model: DeepSeek-V4-Flash..."] --> RouterProxy["gpu-vllm-router (Port 8000)"]
    
    subgraph MultiModelDispatcher ["多模型动态分发网关"]
        Inspector["请求体嗅探器 (JSON Body Inspector)"]
        Registry["集群模型路由注册表 (Dynamic Registry)"]
        PoolA["DeepSeek 模型池<br/>(策略: 一致性哈希)"]
        PoolB["Qwen 模型池<br/>(策略: P2C / Cache-Aware)"]
        PoolC["Llama 模型池<br/>(策略: 轮询)"]
    end

    subgraph GPUWorkers ["GPUStack 各物理节点"]
        D1["DeepSeek 实例 1<br/>192.168.1.10:40039"]
        D2["DeepSeek 实例 2<br/>192.168.1.11:40039"]
        Q1["Qwen 实例 1<br/>192.168.1.12:40039"]
        L1["Llama 实例 1<br/>192.168.1.13:40039"]
    end

    RouterProxy --> Inspector
    Inspector --> Registry
    Registry -->|命中 DeepSeek| PoolA
    Registry -->|命中 Qwen| PoolB
    Registry -->|命中 Llama| PoolC

    PoolA --> D1
    PoolA --> D2
    PoolB --> Q1
    PoolC --> L1
```

1. **按模型隔离的独立负载均衡算法**：每个模型拥有自己独立的均衡池（如活跃连接数追踪、独立一致性哈希环）。不同模型之间的并发请求不会相互污染权重。
2. **零配置扩缩容感知**：当 GPUStack 新增模型部署上线，或某个模型副本数从 1 扩容到 4，Watcher 轮询周期（默认 10 秒）会自动捕捉变化并原子更新内存路由表，完全不需要重启服务。
3. **未就绪模型防击穿保护**：如果请求的模型在 GPUStack 中未找到或可用副本为 0，网关直接返回 OpenAI 规范的 HTTP 404 错误（`The model 'xxx' does not exist or has no healthy backends`），避免请求盲目等待超时。

---

## 六、零停机热重载（Zero-Downtime）原理解析

### 为什么常规直接杀死重启会影响业务？
1. **端口瞬断**：旧进程退出到操作系统解绑端口、新进程启动绑定有 0.5s~2s 的空窗期，新请求必然会报错 `Connection Refused`。
2. **长文本生成断流**：LLM 生成通常持续数秒到数分钟，直接终止旧进程会导致正在吐字的流式长连接直接收到 `Connection Reset by Peer`，导致用户客户端报错。

### 零停机平滑滚动热重载工作流程
```
[客户端请求] ----> 端口 8000 (Go 前置网关永久常驻，永不重启)
                      |
                      |--- (日常运行) ---> [vllm-router 实例 A (端口 18001)] ---> GPU Workers
                      |
              (检测到 GPUStack 实例扩容/缩容)
                      |
                      | 1. 在空闲端口 18002 启动 [vllm-router 实例 B]
                      | 2. 对 18002 发起探活检测 (/health, /v1/models)
                      | 3. 探活通过后，原子切流 (Atomic Switch):
                      |
                      |=== (所有新请求瞬间导向新路由) ===> [vllm-router 实例 B (端口 18002)]
                      |
                      |--- (存量在途请求继续保持) -------> [vllm-router 实例 A (进入 60s 优雅排空)]
                                                                    |
                                                            (60s 到期后平滑安全退出)
```

1. **业务端口永久在线**：对外暴露端口在启动后永久不关断，客户端连接 100% 成功。
2. **候选实例就绪校验**：只有新启动的 `vllm-router` 进程通过健康探活后才切流，杜绝“新进程启动失败导致整网不可用”的风险。
3. **在途长连接完整保障**：旧进程存活直至优雅排空超时（默认 60s），流式 Token 完整推送给调用方。

---

## 七、熔断器（Circuit Breaker）与透明故障转移（Failover Retry）

在大模型推理集群中，部分 Worker 节点可能会因显存 OOM、硬件故障或管理员维护操作而发生**进程崩溃或重启**。为了解决传统轮询在 0~10 秒检测真空期内导致客户端收到 `502 Bad Gateway` 的问题，`gpu-vllm-router` 内置了**高并发断路器与透明故障转移状态机**：

### 1. 三态断路器状态机 (Circuit Breaker State Machine)

每个后端节点（Worker Endpoint）均绑定独立的断路器状态跟踪：

- 🟢 **CLOSED (健康闭合)**：正常接收并均衡处理请求，记录成功与失败次数；
  - **RWMutex 读锁快速路径 (Fast-Path)**：99.99% 的健康常态流量仅需获取读锁即可放行，经基准测试单次检查耗时仅 **50.38 ns**，实现 **0 字节内存分配 (0 B/op, 0 allocs/op)** 与 **29,760,000+ QPS** 的超高并发吞吐能力。
- 🔴 **OPEN (熔断断开)**：当某个节点连续发生 $N$ 次网络连接失败（如 Connection Refused、Dial Timeout）或 5xx 错误时，系统**毫秒级**将该节点标记为 OPEN，并立即将其从可用路由环/池中剔除，无需等待 10s 的 GPUStack API 轮询！
- 🟡 **HALF_OPEN (半开试探)**：进入冷却时间（默认 10s）后，断路器进入半开状态，允许后台主动健康探针（GET /health）或单次测试流量试探后端；一旦验证通过，自动恢复为 CLOSED。

### 2. 透明故障转移与无感重试 (Zero-Downtime Failover Retry)

- 当请求发往某节点并在握手/拨号阶段遭遇网络异常时，反向代理**不会直接返回 502 错误**；
- 系统记录该节点失败一次，并在**完全未向客户端写入数据的前提下**，毫秒级从同模型的其余健康可用节点中重新选取目标进行重试（默认最大重试 2 次）；
- **业务收益**：集群中即便有实例突发宕机或正在重启，调用方依然能获得 100% 的请求成功率与 HTTP 200 返回，业务端完全无感知！

### 3. 主动后台自愈探针 (Proactive Health Probing)

- 处于 OPEN 状态的故障节点，系统会在后台每隔数秒（默认 3s）主动探测其健康端点；
- 一旦节点重启完毕并开始响应，系统立即先于 GPUStack 控制面完成状态自愈（恢复为 CLOSED），毫秒级重新引入流量。

---

## 八、大模型权重加载与深度就绪探测（Readiness Probing）

在生产大模型服务调度中，容器启动与模型权重载入显存存在数十秒到数分钟的时间差：
- **存活探针 (Liveness)**：容器启动后，HTTP 端口即开启监听，`/health` 接口往往立即返回 `HTTP 200`，但此时显存权重仍在大文件拉取或加载中，无法执行推理；
- **就绪探针 (Readiness)**：只有当模型文件完整加载进显卡，`/v1/models` 端点能返回有效模型元数据时，实例才真正具备对外提供服务的能力。

```mermaid
flowchart TD
    NewWorker["新增 Worker 实例上线 (如 9 扩容到 17 实例)"] --> Init["WorkerBreaker 初始化为 Healthy: false"]
    
    Init --> Probe{"就绪探测器 Readiness Probe<br/>GET /v1/models"}
    
    Probe -->|HTTP 503 / 响应为空| Loading["模型权重仍在加载中 (Loading Weights)"]
    Loading --> Wait["保持 Healthy: false / 暂不纳入调度环"]
    Wait -.->|3s 后重试探活| Probe
    
    Probe -->|HTTP 200 & data 包含模型卡片| Ready["模型权重载入完毕 / 推理就绪"]
    Ready --> Healthy["标记 Healthy: true / 准入就绪池"]
    
    Healthy --> WatcherCheck{"健康节点数量是否变动?<br/>(如 9 -> 17)"}
    WatcherCheck -->|是| TriggerReload["触发零停机蓝绿无损重载<br/>(启动候选进程 18002 接收 17 实例流量)"]
    WatcherCheck -->|否| NormalRoute["正常路由调度"]
```

### 核心收益：
1. **解决 9→17 扩容状态撕裂**：彻底避免了扩容期间新实例权重正在加载时，健康计数过早显示为 17 但路由实际活跃仍为 9（`health: 17, active: 9`）的状态撕裂问题；
2. **候选路由器零误杀**：杜绝因新实例尚未完全加载就绪而触发无效的蓝绿重载，防止候选进程因探活超时被意外杀死。

---

## 九、无人值守自动化故障自愈（Auto-Heal）机制

针对大模型生产推理中高发的 **显存枯竭（CUDA OOM）**、**分布式通信死锁（NCCL Error）** 等突发故障，`gpu-vllm-router` 内置了生产级**无人值守自动化故障自愈中枢 (`Auto-Healer`)**：

```mermaid
flowchart TD
    BreakerTrip["Worker 熔断器触发 OPEN (连续失败 >= 3)"] --> AutoHealCheck{"Auto-Heal 开启 &&<br/>实例未被锁定?"}
    
    AutoHealCheck -->|否| Skip["常规断路器隔离 (等待人工介入)"]
    AutoHealCheck -->|是| FetchLogs["通过 GPUStack API 读取容器最近 100 行崩溃日志"]
    
    FetchLogs --> PatternMatch{"正则匹配致命异常?<br/>CUDA OOM / NCCL Error /<br/>CUDA driver error / Bus error"}
    
    PatternMatch -->|无致命错误| RegularProbe["非崩溃故障，保持常规主动探针"]
    PatternMatch -->|命中致命特征| BootGuard{"冷启动安全检查 (Booting Guard)<br/>实例状态是否为 downloading / starting?"}
    
    BootGuard -->|是| SkipReboot["冷启动下载/启动中，豁免重启，等待自然加载"]
    BootGuard -->|否| CooldownCheck{"是否处于重启冷却期 (默认 60s)?<br/>或超过最大重试限制 (默认 3 次)?"}
    
    CooldownCheck -->|是| FlappingProtect["防频繁抖动保护 (Flapping Protection)，跳过重启"]
    CooldownCheck -->|否| ExecReboot["调用 GPUStack API 触发实例软重启 (Reboot)"]
    
    ExecReboot --> Archive["持久化现场取证审计报告<br/>logs/crashes/<model>/<instance>_<timestamp>.log"]
```

### 关键特性说明：
1. **冷启动保护 (Booting Guard)**：对状态仍处于 `downloading`（模型拉取中）或 `starting`（实例初始启动）的节点自动放行，避免将正常冷启动误判为崩溃；
2. **防频繁抖动保护 (Flapping Protection)**：内置 `restart_cooldown`（默认 60s）与 `max_restart_attempts`（默认 3 次），避免因模型权重或参数配置错误导致无限死循环重启；
3. **现场事故取证审计落盘**：触发重启的同时，将发生 OOM 时的详细现场日志、触发原因、实例 ID 及时间戳写入 `logs/crashes/<model>/<instance>_<timestamp>.log`，支持容器持久化挂载，事故排查有据可查。

---

## 十、Prometheus 监控体系与核心指标规范（Port 29000）

为了满足云原生可观测性要求，`gpu-vllm-router` 采用**双网络端口体系**：
- **业务主端口 (`-port 8000`)**：处理高频推理请求转发、Swagger API 调试文档；
- **监控专用端口 (`-metrics-port 29000`)**：独立对外暴露 Prometheus 指标抓取端点，避免监控爬取对推理业务并发造成锁争用与阻塞。

### 1. Prometheus 抓取配置示例 (`prometheus.yml`)

```yaml
scrape_configs:
  - job_name: "vllm-router"
    scrape_interval: 5s
    static_configs:
      - targets: ["<ROUTER_IP>:29000"]
```

### 2. 官方标准指标库对照表

指标命名完全对齐并兼容官方 `vllm-project/router` 体系：

| 指标名称 | 类型 | 核心标签 (Labels) | 含义说明与生产价值 |
| :--- | :---: | :--- | :--- |
| **`vllm_router_running_requests`** | Gauge | `worker="http://..."` | **各 Worker 当前正在处理中的实时在途请求数**。<br>直接反映各显卡当前的动态负载压力。 |
| **`vllm_router_processed_requests_total`** | Counter | `worker="http://..."` | **各 Worker 历史累计处理成功的请求数**。<br>用于计算吞吐率 QPS 与流量倾斜度。 |
| **`vllm_router_cb_outcomes_total`** | Counter | `worker="..."`, `outcome="success\|failure"` | **各 Worker 熔断器记录的调用结果统计**。<br>监控后端节点网络异常率与 HTTP 5xx 故障率。 |
| **`vllm_router_cb_state`** | Gauge | `worker="http://..."` | **各 Worker 当前熔断器健康状态**。<br>取值：`0 = CLOSED (健康)`, `1 = HALF_OPEN (探测中)`, `2 = OPEN (熔断隔离)`。 |
| **`vllm_router_active_workers`** | Gauge | `model="..."` | **各模型当前激活配置的 Worker 实例总数**。 |
| **`vllm_router_healthy_workers`** | Gauge | `model="..."` | **各模型当前经就绪探测确认健康的 Worker 实例数量**。 |

### 3. 常用 PromQL 监控与告警表达式

- **计算整体推理 QPS 吞吐率**：
  ```promql
  sum(rate(vllm_router_processed_requests_total[1m]))
  ```
- **实时在途并发请求总数**：
  ```promql
  sum(vllm_router_running_requests)
  ```
- **后端 Worker 节点熔断告警规则 (Prometheus AlertRule)**：
  ```yaml
  alert: WorkerCircuitBreakerOpen
  expr: vllm_router_cb_state == 2
  for: 30s
  labels:
    severity: warning
  annotations:
    summary: "Worker 节点 {{ $labels.worker }} 发生连续故障，已被断路器隔离！"
  ```

---

## 十一、命令行参数与环境变量全集

| 参数名称 | 环境变量对应 | 默认值 | 详细说明 |
| :--- | :--- | :--- | :--- |
| `-model` | - | *(空)* | **选填**。留空自动开启【全集群多模型自动纳管】；指定名称锁定单模型 |
| `-mode` | - | `cmd` | 运行模式：`cmd` (输出启动命令), `proxy` (内置多模型网关), `run` (守护官方路由器) |
| `-policy` | - | `consistent_hash` | 负载策略：`consistent_hash`, `cache_aware`, `round_robin`, `power_of_two`, `random`, `rendezvous_hash` |
| `-gpustack-url` | `GPUSTACK_BASE_URL` | `http://127.0.0.1:8200` | GPUStack API 服务地址 |
| `-username` | `GPUSTACK_USERNAME` | `admin` | GPUStack 登录管理员用户名 |
| `-password` | `GPUSTACK_PASSWORD` | *(空)* | GPUStack 登录密码 |
| `-api-key` | `GPUSTACK_API_KEY` | *(空)* | GPUStack 认证 Token (如有，优先于用户名密码) |
| `-port` | - | `8000` | 路由器对外监听业务与 API 的主端口 |
| `-host` | - | `0.0.0.0` | 路由器业务服务对外绑定的网络地址 |
| `-metrics-port` | `METRICS_PORT` | `29000` | **Prometheus 监控指标专用服务端口** (避免与业务请求锁争用) |
| `-metrics-host` | `METRICS_HOST` | `0.0.0.0` | Prometheus 监控指标监听网络地址 |
| `-router-bin` | - | `vllm-router` | 官方 `vllm-router` 可执行文件所在路径或名称 |
| `-backend` | - | `vllm` | 后端推理引擎类型 (`vllm`, `sglang`, `trtllm`, `openai`, `anthropic`) |
| `-log-level` | - | `info` | 路由器日志输出级别 (`debug`, `info`, `warn`, `error`) |
| `-log-dir` | - | *(空)* | 路由器全局日志文件存储目录 |
| `-watch-interval` | - | `10s` | 定时向 GPUStack 查询实例拓扑变动的周期 (如 10s, 30s) |
| `-zero-downtime` | - | `true` | `run` 模式下是否启用蓝绿双进程零停机无损热重载 |
| `-drain-timeout` | - | `60s` | `run` 模式切流后，给旧路由器进程保留的流式长连接排空等待时间 |
| `-health-check-timeout` | - | `30s` | 蓝绿候选进程健康检查就绪超时时间 |
| `-auto-heal-enabled` | - | `true` | 是否启用无人值守大模型 CUDA OOM 自动自愈 |
| `-auto-heal-max-restarts` | - | `3` | 故障实例最大连续自动重启次数限制 (防频繁抖动) |
| `-auto-heal-cooldown` | - | `60s` | 两次自愈重启之间的冷却时间间隔 |
| `-auto-heal-log-dir` | - | `logs/crashes` | 故障发生时现场崩溃日志与审计文件落盘目录 |
| `-dp-size` | - | `1` | 数据并行副本数 (Intra-Node Data Parallel Size) |
| `-balance-abs-threshold` | - | `0` (默认64) | 官方 `vllm-router` 负载绝对差均衡阈值 (针对 `cache_aware`) |
| `-balance-rel-threshold` | - | `0.0` (默认1.5) | 官方 `vllm-router` 负载相对比均衡阈值 |
| `-cache-threshold` | - | `0.0` (默认0.3) | 前缀缓存命中复用匹配阈值 (0.0 ~ 1.0) |
| `-eviction-interval` | - | `0` (默认120) | 缓存近似树淘汰操作周期秒数 |
| `-max-tree-size` | - | `0` (默认67108864) | 缓存感知路由近似树最大节点容量 |
| `-max-payload-size` | - | `0` (默认512MB) | 最大单请求体载荷字节数大小 |
| `-vllm-pd-disaggregation` | - | `false` | 是否开启 vLLM Prefill-Decode 两阶段解耦分离路由模式 |
| `-vllm-discovery-address` | - | *(空)* | ZMQ 服务发现协调注册地址 (如 `0.0.0.0:30001`) |
| `-prefill-policy` | - | *(空)* | PD 模式下 Prefill 节点调度策略 |
| `-decode-policy` | - | *(空)* | PD 模式下 Decode 节点调度策略 |
| `-extra-args` | - | *(空)* | 额外自定义 CLI 参数透传 (空格分隔) |
| `-list-models` | - | `false` | 快速列出 GPUStack 集群当前所有模型及其状态后退出 |
| `-list-policies` | - | `false` | 快速列出所有受支持负载均衡模式的详细说明后退出 |

---

## 十二、配置文件使用 (`config.yaml`)

除了命令行参数外，推荐使用 `config.yaml` 统一配置，可直接参考并复用 [`config.example.yaml`](config.example.yaml)：

```yaml
# 核心字段速览 (详见 config.example.yaml)
gpustack:
  base_url: "http://<GPUSTACK_HOST>:8200"
  username: "admin"
  password: "your_password"

target:
  model_name: ""               # 留空代表全集群多模型自动聚合
  policy: "consistent_hash"    # 默认负载调度策略

router:
  mode: "proxy"               # cmd | proxy | run
  host: "0.0.0.0"
  port: 8000                  # 业务网关主端口
  backend: "vllm"             # vllm | sglang | trtllm | openai | anthropic
  log_level: "info"           # debug | info | warn | error
  zero_downtime: true
  drain_timeout: 60s
  health_check_timeout: 30s

metrics:
  port: 29000                 # Prometheus 监控指标独立端口
  host: "0.0.0.0"

circuit_breaker:
  enabled: true               # 是否开启断路器与透明重试
  max_failures: 3             # 连续失败熔断阈值
  cooldown: 10s               # 熔断隔离冷却时间
  max_retries: 2              # 自动透明重试转移次数
  health_check_interval: 3s   # 隔离节点后台自愈嗅探周期

auto_heal:
  enabled: true               # 是否开启无人值守故障自愈
  max_restart_attempts: 3     # 单实例连续重启上限 (防抖保护)
  restart_cooldown: 60s       # 重启冷却时间
  log_dir: "logs/crashes"     # 故障取证日志落盘目录

# 支持针对个别模型进行独立规则与隔离日志目录覆盖：
# models:
#   - model_name: "DeepSeek-V4-Flash-0731-w8a8"
#     policy: "cache_aware"
#     cache_threshold: 0.6
#     log_dir: "logs/deepseek"
```

---

## 十三、管理、监控与 API 文档接口

程序对外提供分端口的管理、监控与交互式 API 文档：

### 1. 业务主端口 (`:8000`)
| 端点路径 | 请求方法 | 说明 |
| :--- | :---: | :--- |
| `/v1/chat/completions` | `POST` | OpenAI 兼容聊天接口，按请求体 `model` 自动分发，支持 SSE 流式传输与长连接复用 |
| `/v1/completions` | `POST` | OpenAI 兼容文本补全接口 |
| `/v1/embeddings` | `POST` | OpenAI 兼容向量计算接口 |
| `/v1/models` | `GET` | OpenAI 兼容模型列表接口，自动聚合全集群所有就绪的模型供客户端选择 |
| `/docs` 或 `/swagger/` | `GET` | **Swagger UI 交互式 API 调试文档**，支持在线 "Try it out" 调试与架构说明 |
| `/redoc` | `GET` | **ReDoc 现代化技术参考文档** |
| `/openapi.json` | `GET` | OpenAPI 3.0.3 规范描述文件（支持直接导入 Postman / Apifox） |
| `/api/topology` | `GET` | 查看集群模型拓扑、在线 Worker 节点与熔断状态 |
| `/api/config` | `GET` / `POST` | 查看当前生效配置或热更新配置 |
| `/api/models/rule` | `POST` | 动态新增或修改特定模型的路由规则与阈值 |
| `/health` | `GET` | 路由器健康检查接口，正常返回 `{"status":"ok"}` |
| `/admin/stats` | `GET` | *(Proxy 模式)* 查看全集群各模型 Worker 列表、健康状态、熔断状态及活跃连接数 |
| `/admin/supervisor` | `GET` | *(Run 模式)* 查看 Supervisor 守护状态、当前主力内部端口及后端 Worker 总数 |

### 2. 监控专用端口 (`:29000`)
| 端点路径 | 请求方法 | 说明 |
| :--- | :---: | :--- |
| `/metrics` | `GET` | **Prometheus 官方规范指标库**，供 Prometheus Server 定期抓取 |

> 离线规范文件已归档于 [`docs/openapi.json`](docs/openapi.json) 与 [`docs/openapi.yaml`](docs/openapi.yaml)，详细说明请参见 [API 文档指南](docs/README.md)。

---

## 十四、生产部署与运维建议

### 1. Linux Systemd 守护进程托管示例
在生产 Linux 环境中，可编写 `/etc/systemd/system/gpu-vllm-router.service`：

```ini
[Unit]
Description=GPUStack to vLLM Router High-Availability Bridge
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/gpu-vllm-router
ExecStart=/opt/gpu-vllm-router/gpu-vllm-router \
    -config /opt/gpu-vllm-router/config.yaml
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 2. 多轮对话与 KV-Cache 复用调优
- 强烈建议业务客户端在请求时带上 HTTP 请求头 `X-Session-ID: <session_uuid>`。
- 在 `consistent_hash` 策略下，拥有相同 `X-Session-ID` 的请求将始终落入同一张显卡/节点，使大模型上下文命中率提升 70%~95% 以上，推理首字延迟大幅降低。

---

## 十五、编译、测试与性能基准

本项目采用 Go 官方标准库编写，零冗余第三方框架依赖，支持跨平台一键编译：

```bash
# 1. 运行所有单元测试与端到端集成测试 (100% 通过)
go test -v ./pkg/...

# 2. 运行高并发性能基准压测
go test -run=^$ -benchmem -bench=. ./pkg/proxy/...

# 3. 编译 Windows 平台可执行文件
go build -o gpu-vllm-router.exe ./cmd/gpu-vllm-router

# 4. 交叉编译 Linux 平台二进制 (部署至 GPU 服务器)
$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o gpu-vllm-router-linux ./cmd/gpu-vllm-router
```

### 性能基准测试结果 (Benchmark Results)
在真实基准压测下，各项核心微架构优化表现优异：
- **断路器无锁并发检查**：**50.38 ns/op**，**0 内存分配 (0 B/op, 0 allocs/op)**，支持 **29,760,000+ QPS** 并发吞吐；
- **请求体单次上下文读取**：从 111.5 µs 降低至 15.5 µs（**7.2 倍提速**），堆内存分配减少 **3.9 倍**（由 112 KB/op 锐减至 28.5 KB/op）；
- **长连接池优化**：`NewOptimizedTransport` 提供 2048+ 每主机长连接常驻复用，彻底消除了高压下的 `TIME_WAIT` 堆积与握手抖动。

---

## 十六、Docker 与 Docker-Compose 容器化部署

项目提供生产级多阶段构建 `Dockerfile` 与 `Dockerfile.cn`（内置最新官方 `vllm-router` Rust 二进制与 Go 调度服务）和支持动态挂载配置文件的 `docker-compose.yml` / `docker-compose.cn.yml`。

针对国内 GPU 服务器网络环境，构建配置已全面内置国内加速源：
- **Debian APT**：阿里云镜像源（`mirrors.aliyun.com`），附带 `[trusted=yes]` 规避 `NO_PUBKEY` GPG 签名报错。
- **Go 依赖**：国内七牛云代理（`goproxy.cn`）。
- **Rust / Cargo**：国内社区稀疏索引源（`rsproxy.cn`）。
- **GitHub 源码**：自动使用 GitHub 镜像加速拉取。

### 1. 目录文件准备
首次从 GitHub 克隆项目后，请复制示例配置并准备日志持久化目录：
```bash
# 1. 从示例模板创建实际配置文件 (已加入 .gitignore，不会被提交)
cp config.example.yaml config.yaml
# 编辑 config.yaml 填入 GPUStack 地址与账号密码

# 2. 创建宿主机日志持久化存储目录 (确保崩溃自愈审计报告与服务日志不随容器销毁而丢失)
mkdir -p logs
```
- `Dockerfile` / `Dockerfile.cn` / `Dockerfile.fast.cn`：全套国内加速多阶段构建文件（预置 `/app/logs` 数据卷）。
- `docker-compose.yml` / `docker-compose.cn.yml`：容器编排定义（挂载 `./config.yaml`、`./logs`，映射 8000 与 29000 端口）。
- `config.yaml`：实际运行配置文件（挂载进容器）。
- `logs/`：宿主机日志与崩溃自愈审计持久化存储目录。

### 2. 一键启动容器

#### 国内服务器标准启动（推荐）：
```bash
# 若服务器 Docker Daemon 版本较低提示 h2c 404，建议先执行 export DOCKER_BUILDKIT=0
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

# 一键拉取源码、编译并启动
docker compose -f docker-compose.cn.yml up -d --build
# 或直接使用默认配置: docker compose up -d --build
```

#### 常用维护命令：
```bash
# 查看容器运行日志
docker compose logs -f

# 检查容器运行状态与健康检查探针
docker compose ps
```

### 3. 常见构建报错排查与解决

#### 1) 提示 `unable to upgrade to h2c, received 404`
- **原因**：服务器 Docker Daemon 未开启 BuildKit 协议支持。
- **解决**：在终端执行 `export DOCKER_BUILDKIT=0; export COMPOSE_DOCKER_CLI_BUILD=0` 即可关闭 BuildKit，使用经典稳定引擎构建。

#### 2) 提示 `NO_PUBKEY ... is not signed`
- **原因**：Debian 官方镜像站海外连接受限或容器内 Keyring 验证异常。
- **解决**：项目中的 `Dockerfile` 和 `Dockerfile.cn` 已预置阿里云源与 `[trusted=yes]`，无需手动配置。

### 4. 配置文件与日志持久化挂载
`docker-compose.yml` 默认挂载宿主机的配置文件与日志持久化目录，并开放业务（8000）与指标（29000）双端口：
```yaml
ports:
  - "8000:8000"     # OpenAI 业务网关主端口
  - "29000:29000"   # Prometheus 监控指标专用端口
volumes:
  # 挂载宿主机的 config.yaml 配置文件 (只读)
  - ./config.yaml:/app/config.yaml:ro
  # 挂载日志持久化目录 (读写，持久化崩溃自愈报告 logs/crashes 与服务运行日志)
  - ./logs:/app/logs
```
- **崩溃取证落盘**：当发生 CUDA OOM、NCCL 通信异常或持久死锁时，自愈系统生成的审计报告（`logs/crashes/<model>/<instance>_<timestamp>.log`）将直接持久化于宿主机的 `./logs/crashes/` 目录下，即便容器重启或销毁重建，现场排查数据绝不丢失。
- **配置热重载**：当您在宿主机修改 `./config.yaml`（例如调整调度策略或增加模型覆盖规则）后，只需平滑重启容器即可生效：
```bash
docker compose restart
```

#### 纯 Docker CLI 单容器直接运行：
若不使用 Docker Compose，亦可通过 `docker run` 直接挂载配置与日志目录并映射双端口：
```bash
docker run -d --name gpu-vllm-router \
  --restart unless-stopped \
  -p 8000:8000 \
  -p 29000:29000 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/logs:/app/logs \
  gpu-vllm-router:latest
```

### 5. 高性能网络模式（Host Network）
若您的 Docker 运行在 GPU 推理主机上，希望省去 Docker Bridge 桥接网络的虚拟化开销，可在 `docker-compose.yml` 中取消注释 `network_mode: "host"`，使容器直接使用宿主机网络栈，获得极限吞吐与极低延迟。



