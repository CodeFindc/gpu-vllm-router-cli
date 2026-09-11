package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gpu-vllm-router/pkg/config"
	"gpu-vllm-router/pkg/gpustack"
	"gpu-vllm-router/pkg/proxy"
	"gpu-vllm-router/pkg/router"
)

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func printHelpBanner() {
	fmt.Println(`===================================================================`)
	fmt.Println(`   GPUStack -> vLLM-Router 动态实例发现与调度工具 (CLI)`)
	fmt.Println(`===================================================================`)
}

func main() {
	// Config File Flag
	configPath := flag.String("config", "", "YAML 配置文件路径 (例如 config.yaml)")

	// GPUStack Flags
	gpustackURL := flag.String("gpustack-url", getEnv("GPUSTACK_BASE_URL", "http://127.0.0.1:8200"), "GPUStack API 服务地址")
	apiKey := flag.String("api-key", getEnv("GPUSTACK_API_KEY", ""), "GPUStack API Key (如有)")
	username := flag.String("username", getEnv("GPUSTACK_USERNAME", "admin"), "GPUStack 登录用户名")
	password := flag.String("password", getEnv("GPUSTACK_PASSWORD", ""), "GPUStack 登录密码")

	// Model Flags
	modelName := flag.String("model", "", "GPUStack 中已启动的目标模型名称 (如: DeepSeek-V4-Flash-0731-w8a8)")
	listModels := flag.Bool("list-models", false, "列出 GPUStack 当前所有已注册模型及其实例状态")

	// Policy Flags
	policyStr := flag.String("policy", "consistent_hash", "负载均衡策略: consistent_hash, round_robin, cache_aware, power_of_two, random, rendezvous_hash")
	listPolicies := flag.Bool("list-policies", false, "显示所有受支持的负载均衡策略及其适用场景")

	// Operating Mode
	mode := flag.String("mode", "cmd", "运行模式: cmd (输出启动命令), proxy (启动内置Go高性能负载均衡代理), run (守护并运行官方vllm-router)")

	// Router / Server Flags
	routerBin := flag.String("router-bin", "vllm-router", "vllm-router 可执行文件路径")
	host := flag.String("host", "0.0.0.0", "路由器绑定监听地址")
	port := flag.Int("port", 8000, "路由器绑定监听端口")
	watchInterval := flag.Duration("watch-interval", 10*time.Second, "动态实例状态感知轮询间隔 (例如 10s, 30s)")
	dpSize := flag.Int("dp-size", 1, "数据并行度 (intra-node data parallel size)")
	zeroDowntime := flag.Bool("zero-downtime", true, "run 模式是否启用蓝绿双进程零停机平滑滚动热重载 (推荐开启)")
	drainTimeout := flag.Duration("drain-timeout", 60*time.Second, "run 模式热重载时旧进程的优雅排空等待超时时间")

	// Tuning Flags for official vllm-router
	balAbsThresh := flag.Int("balance-abs-threshold", 0, "vllm-router 负载绝对差均衡阈值 (如 4)")
	balRelThresh := flag.Float64("balance-rel-threshold", 0.0, "vllm-router 负载相对比均衡阈值 (如 1.1)")
	cacheThresh := flag.Float64("cache-threshold", 0.0, "vllm-router 前缀缓存命中复用阈值 (如 0.6)")
	evictionIntervalFlag := flag.Int("eviction-interval", 0, "前缀缓存淘汰间隔秒数 (如 120)")
	maxTreeSizeFlag := flag.Int("max-tree-size", 0, "前缀缓存近似树最大容量 (如 67108864)")
	maxPayloadSizeFlag := flag.Int("max-payload-size", 0, "最大请求体载荷字节大小 (如 536870912)")

	// Engine & Logging
	backendFlag := flag.String("backend", "vllm", "后端推理引擎类型 (vllm, sglang, trtllm, openai, anthropic)")
	logLevelFlag := flag.String("log-level", "info", "路由器日志级别 (debug, info, warn, error)")
	logDirFlag := flag.String("log-dir", "", "路由器日志存储目录")

	// PD Disaggregation Flags
	pdDisaggFlag := flag.Bool("vllm-pd-disaggregation", false, "是否开启 Prefill-Decode 两阶段分离路由模式")
	discoveryAddrFlag := flag.String("vllm-discovery-address", "", "ZMQ 服务发现注册通信地址 (如 0.0.0.0:30001)")
	prefillPolicyFlag := flag.String("prefill-policy", "", "PD 模式下 Prefill 节点调度策略")
	decodePolicyFlag := flag.String("decode-policy", "", "PD 模式下 Decode 节点调度策略")

	// Worker startup flags
	workerStartupTimeoutFlag := flag.Int("worker-startup-timeout-secs", 0, "Worker 实例启动等待超时时间秒数")
	workerStartupCheckIntervalFlag := flag.Int("worker-startup-check-interval", 0, "Worker 实例就绪探测轮询周期秒数")

	extraArgsFlag := flag.String("extra-args", "", "vllm-router 额外自定义 CLI 参数 (空格分隔)")

	flag.Parse()

	// Track explicitly provided CLI flags
	explicitFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})

	// Load configuration file if specified or if default config.yaml exists
	actualConfigPath := *configPath
	if actualConfigPath == "" && !explicitFlags["model"] && !explicitFlags["gpustack-url"] {
		if _, err := os.Stat("config.yaml"); err == nil {
			actualConfigPath = "config.yaml"
		}
	}

	var fileCfg *config.FileConfig
	if actualConfigPath != "" {
		var err error
		fileCfg, err = config.LoadConfig(actualConfigPath)
		if err != nil {
			log.Fatalf("加载配置文件失败: %v", err)
		}
		log.Printf("已成功载入配置文件: %s", actualConfigPath)

		if !explicitFlags["gpustack-url"] && fileCfg.GPUStack.BaseURL != "" {
			*gpustackURL = fileCfg.GPUStack.BaseURL
		}
		if !explicitFlags["username"] && fileCfg.GPUStack.Username != "" {
			*username = fileCfg.GPUStack.Username
		}
		if !explicitFlags["password"] && fileCfg.GPUStack.Password != "" {
			*password = fileCfg.GPUStack.Password
		}
		if !explicitFlags["api-key"] && fileCfg.GPUStack.APIKey != "" {
			*apiKey = fileCfg.GPUStack.APIKey
		}
		if !explicitFlags["model"] && fileCfg.Target.ModelName != "" {
			*modelName = fileCfg.Target.ModelName
		}
		if !explicitFlags["policy"] && fileCfg.Target.Policy != "" {
			*policyStr = fileCfg.Target.Policy
		}
		if !explicitFlags["mode"] && fileCfg.Router.Mode != "" {
			*mode = fileCfg.Router.Mode
		}
		if !explicitFlags["host"] && fileCfg.Router.Host != "" {
			*host = fileCfg.Router.Host
		}
		if !explicitFlags["port"] && fileCfg.Router.Port > 0 {
			*port = fileCfg.Router.Port
		}
		if !explicitFlags["router-bin"] && fileCfg.Router.RouterBin != "" {
			*routerBin = fileCfg.Router.RouterBin
		}
		if !explicitFlags["watch-interval"] && fileCfg.Router.WatchInterval > 0 {
			*watchInterval = fileCfg.Router.WatchInterval
		}
		if !explicitFlags["dp-size"] && fileCfg.Router.DataParallelSize > 0 {
			*dpSize = fileCfg.Router.DataParallelSize
		}
		if !explicitFlags["zero-downtime"] && fileCfg.Router.ZeroDowntime != nil {
			*zeroDowntime = *fileCfg.Router.ZeroDowntime
		}
		if !explicitFlags["drain-timeout"] && fileCfg.Router.DrainTimeout > 0 {
			*drainTimeout = fileCfg.Router.DrainTimeout
		}
		if !explicitFlags["balance-abs-threshold"] && fileCfg.Router.BalanceAbsThreshold != nil {
			*balAbsThresh = *fileCfg.Router.BalanceAbsThreshold
		}
		if !explicitFlags["balance-rel-threshold"] && fileCfg.Router.BalanceRelThreshold != nil {
			*balRelThresh = *fileCfg.Router.BalanceRelThreshold
		}
		if !explicitFlags["cache-threshold"] && fileCfg.Router.CacheThreshold != nil {
			*cacheThresh = *fileCfg.Router.CacheThreshold
		}
		if !explicitFlags["backend"] && fileCfg.Router.Backend != "" {
			*backendFlag = fileCfg.Router.Backend
		}
		if !explicitFlags["log-level"] && fileCfg.Router.LogLevel != "" {
			*logLevelFlag = fileCfg.Router.LogLevel
		}
		if !explicitFlags["log-dir"] && fileCfg.Router.LogDir != "" {
			*logDirFlag = fileCfg.Router.LogDir
		}
		if !explicitFlags["eviction-interval"] && fileCfg.Router.EvictionInterval != nil {
			*evictionIntervalFlag = *fileCfg.Router.EvictionInterval
		}
		if !explicitFlags["max-tree-size"] && fileCfg.Router.MaxTreeSize != nil {
			*maxTreeSizeFlag = *fileCfg.Router.MaxTreeSize
		}
		if !explicitFlags["max-payload-size"] && fileCfg.Router.MaxPayloadSize != nil {
			*maxPayloadSizeFlag = *fileCfg.Router.MaxPayloadSize
		}
		if !explicitFlags["vllm-pd-disaggregation"] && fileCfg.Router.VllmPDDisagg != nil {
			*pdDisaggFlag = *fileCfg.Router.VllmPDDisagg
		}
		if !explicitFlags["vllm-discovery-address"] && fileCfg.Router.VllmDiscoveryAddress != "" {
			*discoveryAddrFlag = fileCfg.Router.VllmDiscoveryAddress
		}
		if !explicitFlags["prefill-policy"] && fileCfg.Router.PrefillPolicy != "" {
			*prefillPolicyFlag = fileCfg.Router.PrefillPolicy
		}
		if !explicitFlags["decode-policy"] && fileCfg.Router.DecodePolicy != "" {
			*decodePolicyFlag = fileCfg.Router.DecodePolicy
		}
		if !explicitFlags["worker-startup-timeout-secs"] && fileCfg.Router.WorkerStartupTimeoutSecs != nil {
			*workerStartupTimeoutFlag = *fileCfg.Router.WorkerStartupTimeoutSecs
		}
		if !explicitFlags["worker-startup-check-interval"] && fileCfg.Router.WorkerStartupCheckInterval != nil {
			*workerStartupCheckIntervalFlag = *fileCfg.Router.WorkerStartupCheckInterval
		}
	}

	// 1. If user requested --list-policies
	if *listPolicies {
		printHelpBanner()
		fmt.Println("受支持的负载均衡模式说明:")
		fmt.Println("-------------------------------------------------------------------")
		for _, p := range router.SupportedPolicies() {
			fmt.Printf("策略名称: %-16s | 会话保持: %-5t | 负载感知: %-5t\n", p.Name, p.SessionAffinity, p.LoadAware)
			fmt.Printf("  说明: %s\n", p.Description)
			fmt.Printf("  最佳场景: %s\n\n", p.BestFor)
		}
		return
	}

	// Validate Policy
	selectedPolicy, err := router.ParsePolicy(*policyStr)
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}

	// Initialize GPUStack Client
	clientCfg := gpustack.Config{
		BaseURL:  *gpustackURL,
		APIKey:   *apiKey,
		Username: *username,
		Password: *password,
		Timeout:  10 * time.Second,
	}

	client, err := gpustack.NewClient(clientCfg)
	if err != nil {
		log.Fatalf("初始化 GPUStack 客户端失败: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 2. If user requested --list-models
	if *listModels {
		printHelpBanner()
		fmt.Printf("正在从 GPUStack (%s) 获取模型列表...\n", *gpustackURL)
		models, err := client.GetModels(ctx)
		if err != nil {
			log.Fatalf("获取模型列表失败: %v", err)
		}
		fmt.Printf("发现 %d 个模型:\n", len(models))
		fmt.Println("-------------------------------------------------------------------")
		fmt.Printf("%-5s | %-40s | %-8s | %-8s | %-8s\n", "ID", "模型名称", "总副本数", "就绪副本", "后端类型")
		fmt.Println("-------------------------------------------------------------------")
		for _, m := range models {
			fmt.Printf("%-5d | %-40s | %-8d | %-8d | %-8s\n", m.ID, m.Name, m.Replicas, m.ReadyReplicas, m.Backend)
		}
		fmt.Println("-------------------------------------------------------------------")
		return
	}

	printHelpBanner()
	log.Printf("目标 GPUStack 地址: %s", *gpustackURL)
	if *modelName != "" {
		log.Printf("指定目标模型名称:   %s (单模型定向模式)", *modelName)
	} else {
		log.Printf("运行调度模式:       全集群多模型自动同步负载模式 (未指定固定模型，自动纳管所有模型)")
	}
	log.Printf("选定负载均衡策略:   %s", selectedPolicy)
	log.Printf("运行模式:           %s", *mode)

	var workerURLs []string
	var sampleModelName string

	if *modelName != "" {
		// Single Model Mode
		endpoints, model, err := client.GetRunningWorkerEndpoints(ctx, *modelName)
		if err != nil {
			if *mode == "cmd" {
				log.Fatalf("发现模型实例失败: %v", err)
			}
			log.Printf("⚠️ 发现模型实例警告 (启动进入待机模式): %v", err)
		}
		if len(endpoints) == 0 {
			if *mode == "cmd" {
				log.Fatalf("模型 %q 当前没有处于 running 状态的实例工作点！", *modelName)
			}
			log.Printf("⚠️ 模型 %q 当前没有处于 running 状态的实例工作点 (启动进入待机模式)", *modelName)
		} else {
			sampleModelName = model.Name
			fmt.Println("\n================= GPUStack 单模型实例服务发现结果 =================")
			fmt.Printf("模型名称: %s (ID: %d, 后端: %s)\n", model.Name, model.ID, model.Backend)
			fmt.Printf("健康工作点数量: %d\n", len(endpoints))
			fmt.Println("-------------------------------------------------------------------")
			fmt.Printf("%-18s | %-15s | %-16s | %-6s | %s\n", "实例名称", "Worker 节点", "IP 地址", "端口", "工作点接入 URL")
			fmt.Println("-------------------------------------------------------------------")
			for _, ep := range endpoints {
				fmt.Printf("%-18s | %-15s | %-16s | %-6d | %s\n", ep.InstanceName, ep.WorkerName, ep.IP, ep.Port, ep.URL)
				workerURLs = append(workerURLs, ep.URL)
			}
			fmt.Println("===================================================================")
			fmt.Println()
		}
	} else {
		// Full Cluster Multi-Model Mode
		cluster, err := client.GetAllRunningWorkerEndpoints(ctx)
		if err != nil {
			if *mode == "cmd" {
				log.Fatalf("全集群服务发现失败: %v", err)
			}
			log.Printf("⚠️ 全集群服务发现警告 (启动进入待机模式): %v", err)
		}
		if cluster == nil || cluster.InstanceCount == 0 {
			if *mode == "cmd" {
				log.Fatalf("GPUStack 集群当前没有处于 running 状态的任何模型实例！")
			}
			log.Printf("⚠️ GPUStack 集群当前没有处于 running 状态的模型实例 (启动进入待机模式，将在后台自动同步感知)")
		} else {
			fmt.Println("\n================= GPUStack 全集群多模型服务发现结果 =================")
			fmt.Printf("在线模型数量: %d | 运行中工作点总数: %d\n", cluster.ModelCount, cluster.InstanceCount)
			fmt.Println("-------------------------------------------------------------------")
			for mName, eps := range cluster.ModelsEndpoints {
				if sampleModelName == "" {
					sampleModelName = mName
				}
				fmt.Printf("▶ 模型: %s (就绪副本: %d)\n", mName, len(eps))
				for _, ep := range eps {
					fmt.Printf("    -> 实例: %-25s Worker: %-12s IP: %-15s 端口: %-6d URL: %s\n",
						ep.InstanceName, ep.WorkerName, ep.IP, ep.Port, ep.URL)
					workerURLs = append(workerURLs, ep.URL)
				}
			}
			fmt.Println("===================================================================")
			fmt.Println()
		}
	}

	routerCfg := router.Config{
		RouterBin:                  *routerBin,
		Host:                       *host,
		Port:                       *port,
		Policy:                     selectedPolicy,
		WorkerURLs:                 workerURLs,
		DataParallelSize:           *dpSize,
		Backend:                    *backendFlag,
		LogLevel:                   *logLevelFlag,
		LogDir:                     *logDirFlag,
		VllmPDDisagg:               *pdDisaggFlag,
		VllmDiscoveryAddress:       *discoveryAddrFlag,
		PrefillPolicy:              *prefillPolicyFlag,
		DecodePolicy:               *decodePolicyFlag,
		EvictionInterval:           *evictionIntervalFlag,
		MaxTreeSize:                *maxTreeSizeFlag,
		MaxPayloadSize:             *maxPayloadSizeFlag,
		WorkerStartupTimeoutSecs:   *workerStartupTimeoutFlag,
		WorkerStartupCheckInterval: *workerStartupCheckIntervalFlag,
	}

	if fileCfg != nil {
		if len(fileCfg.Router.PrefillURLs) > 0 {
			routerCfg.PrefillURLs = fileCfg.Router.PrefillURLs
		}
		if len(fileCfg.Router.DecodeURLs) > 0 {
			routerCfg.DecodeURLs = fileCfg.Router.DecodeURLs
		}
		if fileCfg.Router.APIKey != "" {
			routerCfg.APIKey = fileCfg.Router.APIKey
		}
		if len(fileCfg.Router.APIKeyValidationURLs) > 0 {
			routerCfg.APIKeyValidationURLs = fileCfg.Router.APIKeyValidationURLs
		}
		if fileCfg.CircuitBreaker.HealthFailureThreshold > 0 {
			routerCfg.HealthFailureThreshold = fileCfg.CircuitBreaker.HealthFailureThreshold
		}
		if fileCfg.CircuitBreaker.HealthSuccessThreshold > 0 {
			routerCfg.HealthSuccessThreshold = fileCfg.CircuitBreaker.HealthSuccessThreshold
		}
		if fileCfg.CircuitBreaker.HealthCheckEndpoint != "" {
			routerCfg.HealthCheckEndpoint = fileCfg.CircuitBreaker.HealthCheckEndpoint
		}
		if fileCfg.CircuitBreaker.DisableCircuitBreaker != nil && *fileCfg.CircuitBreaker.DisableCircuitBreaker {
			routerCfg.DisableCircuitBreaker = true
		}
		if fileCfg.CircuitBreaker.DisableRetries != nil && *fileCfg.CircuitBreaker.DisableRetries {
			routerCfg.DisableRetries = true
		}
		if fileCfg.CircuitBreaker.MaxFailures > 0 {
			routerCfg.CbFailureThreshold = fileCfg.CircuitBreaker.MaxFailures
		}
		if fileCfg.CircuitBreaker.SuccessThreshold > 0 {
			routerCfg.CbSuccessThreshold = fileCfg.CircuitBreaker.SuccessThreshold
		}
		if fileCfg.CircuitBreaker.Cooldown > 0 {
			routerCfg.CbTimeoutDurationSecs = int(fileCfg.CircuitBreaker.Cooldown.Seconds())
		}
		if fileCfg.CircuitBreaker.WindowDuration > 0 {
			routerCfg.CbWindowDurationSecs = int(fileCfg.CircuitBreaker.WindowDuration.Seconds())
		}
		if fileCfg.CircuitBreaker.MaxRetries > 0 {
			routerCfg.RetryMaxRetries = fileCfg.CircuitBreaker.MaxRetries
		}
		if fileCfg.CircuitBreaker.RetryInitialBackoff > 0 {
			routerCfg.RetryInitialBackoffMs = int(fileCfg.CircuitBreaker.RetryInitialBackoff.Milliseconds())
		}
		if fileCfg.CircuitBreaker.HealthCheckInterval > 0 {
			routerCfg.HealthCheckIntervalSecs = int(fileCfg.CircuitBreaker.HealthCheckInterval.Seconds())
		}
		if fileCfg.CircuitBreaker.HealthCheckTimeout > 0 {
			routerCfg.HealthCheckTimeoutSecs = int(fileCfg.CircuitBreaker.HealthCheckTimeout.Seconds())
		}
		if fileCfg.CircuitBreaker.Enabled != nil && !*fileCfg.CircuitBreaker.Enabled {
			routerCfg.DisableCircuitBreaker = true
			routerCfg.DisableRetries = true
		}
	}

	if *balAbsThresh > 0 {
		routerCfg.BalanceAbsThreshold = *balAbsThresh
	}
	if *balRelThresh > 0 {
		routerCfg.BalanceRelThreshold = *balRelThresh
	}
	if *cacheThresh > 0 {
		routerCfg.CacheThreshold = *cacheThresh
	}
	if *extraArgsFlag != "" {
		routerCfg.ExtraArgs = strings.Fields(*extraArgsFlag)
	} else if fileCfg != nil && len(fileCfg.Router.ExtraArgs) > 0 {
		routerCfg.ExtraArgs = fileCfg.Router.ExtraArgs
	}

	switch *mode {
	case "cmd":
		fmt.Println(">>> 1. Linux / macOS (Bash) 启动命令:")
		fmt.Println(router.GenerateBashCommand(routerCfg))
		fmt.Println("\n>>> 2. Windows (PowerShell) 启动命令:")
		fmt.Println(router.GeneratePowerShellCommand(routerCfg))
		fmt.Println("\n>>> 3. Docker 容器化启动命令:")
		fmt.Println(router.GenerateDockerCommand(routerCfg, "vllm/vllm-router:latest"))
		fmt.Println("\n>>> 4. 多模型统一接入测试示例:")
		fmt.Printf("curl -X POST http://127.0.0.1:%d/v1/chat/completions \\\n", *port)
		fmt.Printf("  -H \"Content-Type: application/json\" \\\n")
		fmt.Printf("  -H \"X-Session-ID: session_12345\" \\\n")
		fmt.Printf("  -d '{\"model\": \"%s\", \"messages\": [{\"role\": \"user\", \"content\": \"Hello!\"}]}'\n", sampleModelName)
		fmt.Println("\n>>> 5. API 文档与运维监控接口地址 (在 proxy 或 run 模式下生效):")
		fmt.Printf("  - Swagger UI 交互式: http://127.0.0.1:%d/docs (或 /swagger/)\n", *port)
		fmt.Printf("  - ReDoc 参考文档:   http://127.0.0.1:%d/redoc\n", *port)
		fmt.Printf("  - OpenAPI 3.0 规范: http://127.0.0.1:%d/openapi.json\n", *port)
		fmt.Printf("  - 健康检查探针:     http://127.0.0.1:%d/health\n", *port)
		fmt.Printf("  - 集群拓扑 API:     http://127.0.0.1:%d/api/topology\n", *port)
		fmt.Printf("  - Prometheus 监控:  http://127.0.0.1:%d/metrics\n", *port)

	case "run":
		log.Printf("正在以守护进程模式启动官方 vllm-router (%s)...", *routerBin)
		resolvedConfigPath := actualConfigPath
		if resolvedConfigPath == "" {
			resolvedConfigPath = "config.yaml"
		}
		var modelRules []config.ModelRule
		if fileCfg != nil {
			modelRules = fileCfg.Models
		}

		supCfg := router.SupervisorConfig{
			ZeroDowntime:   *zeroDowntime,
			PublicHost:     *host,
			PublicPort:     *port,
			DrainTimeout:   *drainTimeout,
			WatchInterval:  *watchInterval,
			RouterCfg:      routerCfg,
			ConfigFilePath: resolvedConfigPath,
			ModelRules:     modelRules,
		}
		if fileCfg != nil {
			if fileCfg.CircuitBreaker.HealthCheckInterval > 0 {
				supCfg.WorkerProbeInterval = fileCfg.CircuitBreaker.HealthCheckInterval
			}
			if fileCfg.CircuitBreaker.MaxFailures > 0 {
				supCfg.WorkerMaxFailures = fileCfg.CircuitBreaker.MaxFailures
			}
			supCfg.AutoHeal = fileCfg.AutoHeal
		}
		sup := router.NewSupervisor(client, *modelName, supCfg)
		if err := sup.Start(ctx); err != nil {
			log.Fatalf("启动 vllm-router 守护进程失败: %v", err)
		}
		<-ctx.Done()
		log.Println("正在优雅关闭 vllm-router 守护进程...")
		sup.Stop()

	case "proxy":
		log.Printf("正在启动内置 Go 语言高性能 OpenAI 兼容反向代理网关...")
		resolvedConfigPath := actualConfigPath
		if resolvedConfigPath == "" {
			resolvedConfigPath = "config.yaml"
		}
		var modelRules []config.ModelRule
		if fileCfg != nil {
			modelRules = fileCfg.Models
		}

		cbCfg := proxy.DefaultCircuitBreakerConfig()
		if fileCfg != nil {
			if fileCfg.CircuitBreaker.MaxFailures > 0 {
				cbCfg.MaxFailures = fileCfg.CircuitBreaker.MaxFailures
			}
			if fileCfg.CircuitBreaker.Cooldown > 0 {
				cbCfg.Cooldown = fileCfg.CircuitBreaker.Cooldown
			}
			if fileCfg.CircuitBreaker.MaxRetries > 0 {
				cbCfg.MaxRetries = fileCfg.CircuitBreaker.MaxRetries
			}
			if fileCfg.CircuitBreaker.HealthCheckInterval > 0 {
				cbCfg.HealthCheckInterval = fileCfg.CircuitBreaker.HealthCheckInterval
			}
		}

		var balAbsPtr *int
		if *balAbsThresh > 0 {
			balAbsPtr = balAbsThresh
		}
		var balRelPtr *float64
		if *balRelThresh > 0 {
			balRelPtr = balRelThresh
		}
		var cacheThreshPtr *float64
		if *cacheThresh > 0 {
			cacheThreshPtr = cacheThresh
		}

		proxyCfg := proxy.ServerConfig{
			Host:                *host,
			Port:                *port,
			Policy:              selectedPolicy,
			ModelName:           *modelName,
			WatchInterval:       *watchInterval,
			CircuitBreaker:      cbCfg,
			ConfigFilePath:      resolvedConfigPath,
			ModelRules:          modelRules,
			ZeroDowntime:        *zeroDowntime,
			DrainTimeout:        *drainTimeout,
			BalanceAbsThreshold: balAbsPtr,
			BalanceRelThreshold: balRelPtr,
			CacheThreshold:      cacheThreshPtr,
			ExtraArgs:           routerCfg.ExtraArgs,
		}
		if fileCfg != nil {
			proxyCfg.AutoHeal = fileCfg.AutoHeal
		}
		srv := proxy.NewServer(proxyCfg, client)

		go func() {
			if err := srv.Start(ctx); err != nil {
				log.Fatalf("代理服务异常退出: %v", err)
			}
		}()

		// Wait for shutdown signal
		<-ctx.Done()
		log.Println("正在优雅关闭内置反向代理...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = srv.Stop(shutdownCtx)

	default:
		log.Fatalf("未知的运行模式 %q，仅支持: cmd, proxy, run", *mode)
	}
}
