// 自动扩缩容服务：基于 Redis Streams 的积压监控与容器扩缩
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	// RedisAddr 目标 Redis 地址，例如 "redis:6379"
	RedisAddr string
	// RedisDB 选择的数据库索引
	RedisDB int
	// RedisPassword Redis 认证密码
	RedisPassword string
	// StreamKey 被监控的 Stream key，例如 "judge:task"
	StreamKey string
	// GroupName 监控的消费者组名，例如 "judger_group"
	GroupName string
	// ServiceName 需要扩缩容的 Compose 服务名，例如 "online-judge-judger-worker"
	ServiceName string
	// ProjectName Compose 项目名，用于更精确的标签过滤
	ProjectName string
	// MonitorInterval 调谐循环的周期
	MonitorInterval time.Duration
	// ScaleUpThreshold 阈值策略下的扩容积压阈值
	ScaleUpThreshold int64
	// ScaleDownThreshold 阈值策略下的缩容积压阈值
	ScaleDownThreshold int64
	// MinReplicas 最小副本数
	MinReplicas int
	// MaxReplicas 最大副本数
	MaxReplicas int
	// ScaleUpStep 阈值策略下的每次扩容步进
	ScaleUpStep int
	// ScaleDownStep 阈值策略下的每次缩容步进
	ScaleDownStep int
	// Cooldown 扩缩容冷却期，防止抖动
	Cooldown time.Duration
	// HTTPPort 管理接口端口
	HTTPPort string
	// BacklogPerWorker 目标队列策略：每个 worker 期望承载的积压条目数
	BacklogPerWorker int64
}

type Autoscaler struct {
	// cfg 运行配置
	cfg Config
	// rdb Redis 客户端
	rdb redis.Cmdable
	// docker Docker 客户端
	docker *client.Client
	// mu 保护扩缩容过程的互斥锁
	mu sync.Mutex
	// lastScaleAt 上次扩缩容发生的时间，用于冷却控制
	lastScaleAt time.Time
	autoscalingEnabled bool
}

func (a *Autoscaler) isAutoscalingEnabled() bool {
	a.mu.Lock()
	v := a.autoscalingEnabled
	a.mu.Unlock()
	return v
}

func (a *Autoscaler) setAutoscalingEnabled(v bool) {
	a.mu.Lock()
	a.autoscalingEnabled = v
	a.mu.Unlock()
}

// envInt 从环境变量读取整型值，若不存在或解析失败则返回默认值。
func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

// envInt64 从环境变量读取 int64 值，若不存在或解析失败返回默认值。
func envInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

// envDuration 从环境变量读取持续时间（如 "5s"、"1m"），失败则返回默认值。
func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// loadConfig 加载并组装所有配置，设置合理的默认值以便在 Compose 网络内即开即用。
func loadConfig() Config {
	redisHost := strings.TrimSpace(os.Getenv("REDIS_HOST"))
	if redisHost == "" {
		redisHost = "redis"
	}
	redisPort := envInt("REDIS_PORT", 6379)
	redisAddr := fmt.Sprintf("%s:%d", redisHost, redisPort)
	stream := strings.TrimSpace(os.Getenv("STREAM_KEY"))
	if stream == "" {
		stream = "judge:task"
	}
	group := strings.TrimSpace(os.Getenv("GROUP_NAME"))
	if group == "" {
		group = "judger_group"
	}
	svc := strings.TrimSpace(os.Getenv("SERVICE_NAME"))
	if svc == "" {
		svc = "online-judge-judger-worker"
	}
	httpPort := strings.TrimSpace(os.Getenv("HTTP_PORT"))
	if httpPort == "" {
		httpPort = "8088"
	}
	return Config{
		RedisAddr:          redisAddr,
		RedisDB:            envInt("REDIS_DB", 0),
		RedisPassword:      os.Getenv("REDIS_PASSWORD"),
		StreamKey:          stream,
		GroupName:          group,
		ServiceName:        svc,
		ProjectName:        strings.TrimSpace(os.Getenv("COMPOSE_PROJECT_NAME")),
		MonitorInterval:    envDuration("MONITOR_INTERVAL", 5*time.Second),
		ScaleUpThreshold:   envInt64("SCALE_UP_THRESHOLD", 20),
		ScaleDownThreshold: envInt64("SCALE_DOWN_THRESHOLD", 5),
		MinReplicas:        envInt("MIN_REPLICAS", 5),
		MaxReplicas:        envInt("MAX_REPLICAS", 10),
		ScaleUpStep:        envInt("SCALE_UP_STEP", 1),
		ScaleDownStep:      envInt("SCALE_DOWN_STEP", 1),
		Cooldown:           envDuration("SCALE_COOLDOWN", 20*time.Second),
		HTTPPort:           httpPort,
		BacklogPerWorker:   envInt64("BACKLOG_PER_WORKER", 0),
	}
}

// newRedis 创建 Redis 客户端。
func newRedis(cfg Config) redis.Cmdable {
	return redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB, Password: cfg.RedisPassword})
}

// newDockerClient 创建 Docker 客户端，优先通过环境变量，失败则回退到 Linux 的 unix socket。
func newDockerClient() (*client.Client, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		cli, err = client.NewClientWithOpts(client.WithHost("unix:///var/run/docker.sock"), client.WithAPIVersionNegotiation())
		if err != nil {
			return nil, err
		}
	}
	return cli, nil
}

// pending 读取消费者组的未确认消息总数（积压量），来源于 XPENDING 的 Count。
func (a *Autoscaler) pending(ctx context.Context) (int64, error) {
	res, err := a.rdb.XPending(ctx, a.cfg.StreamKey, a.cfg.GroupName).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return res.Count, nil
}

// currentReplicas 通过 Compose 标签过滤正在运行的目标服务容器，返回副本数与容器列表。
func (a *Autoscaler) currentReplicas(ctx context.Context) (int, []container.Summary, error) {
	filters := client.Filters{}
	filters.Add("label", "com.docker.compose.service="+a.cfg.ServiceName)
	if a.cfg.ProjectName != "" {
		filters.Add("label", "com.docker.compose.project="+a.cfg.ProjectName)
	}
	filters.Add("status", "running")
	list, err := a.docker.ContainerList(ctx, client.ContainerListOptions{
		Filters: filters,
	})
	if err != nil {
		return 0, nil, err
	}
	return len(list.Items), list.Items, nil
}

// nextIndex 基于已有容器名称集合，计算下一个可用的递增索引。
func nextIndex(names []string, base string) int {
	max := 0
	for _, n := range names {
		s := strings.TrimPrefix(n, "/")
		parts := strings.Split(s, "-")
		if len(parts) >= 1 && strings.Contains(s, base) {
			last := parts[len(parts)-1]
			idx, _ := strconv.Atoi(last)
			if idx > max {
				max = idx
			}
		}
	}
	if max == 0 {
		return 1
	}
	return max + 1
}

// scaleTo 将当前副本数调整到目标值 desired（带边界与冷却控制）。
// 扩容：复制一个模板容器的 Config/HostConfig/NetworkingConfig 生成新副本。
// 缩容：优先停止 PEL=0 的空闲消费者对应容器，不足时再按尾部顺序停止。
func (a *Autoscaler) scaleTo(ctx context.Context, desired int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if desired < a.cfg.MinReplicas {
		desired = a.cfg.MinReplicas
	}
	if desired > a.cfg.MaxReplicas {
		desired = a.cfg.MaxReplicas
	}
	if time.Since(a.lastScaleAt) < a.cfg.Cooldown {
		return nil
	}
	cur, list, err := a.currentReplicas(ctx)
	if err != nil {
		return err
	}
	if cur == desired {
		return nil
	}
	if cur < desired {
		if len(list) == 0 {
			return fmt.Errorf("no template container found for service %s", a.cfg.ServiceName)
		}
		tpl := list[0]
		insp, err := a.docker.ContainerInspect(ctx, tpl.ID, client.ContainerInspectOptions{})
		if err != nil {
			return err
		}
		networkName := ""
		for k := range insp.Container.NetworkSettings.Networks {
			networkName = k
			break
		}
		names := make([]string, 0, len(list))
		for _, c := range list {
			names = append(names, c.Names...)
		}
		for i := 0; i < desired-cur; i++ {
			idx := nextIndex(names, a.cfg.ServiceName)
			name := fmt.Sprintf("%s-%d", a.cfg.ServiceName, idx)
			config := insp.Container.Config
			hostCfg := insp.Container.HostConfig
			netCfg := &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{}}
			if networkName != "" {
				netCfg.EndpointsConfig[networkName] = &network.EndpointSettings{}
			}
			config.Hostname = ""
			config.Domainname = ""
			resp, err := a.docker.ContainerCreate(ctx, client.ContainerCreateOptions{
				Name:             name,
				Config:           config,
				HostConfig:       hostCfg,
				NetworkingConfig: netCfg,
			})
			if err != nil {
				return err
			}
			if _, err = a.docker.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{}); err != nil {
				return err
			}
			names = append(names, name)
		}
	} else {
		extra := cur - desired
		filters := client.Filters{}
		filters.Add("label", "com.docker.compose.service="+a.cfg.ServiceName)
		if a.cfg.ProjectName != "" {
			filters.Add("label", "com.docker.compose.project="+a.cfg.ProjectName)
		}
		filters.Add("status", "running")
		containers, err := a.docker.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
		if err != nil {
			return err
		}
		idleIDs, err := a.selectIdleContainers(ctx, extra)
		if err != nil {
			idleIDs = nil
		}
		stopped := 0
		for _, id := range idleIDs {
			if _, err = a.docker.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: ToPtr(10)}); err != nil {
				continue
			}
			if _, err = a.docker.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true}); err != nil {
				continue
			}
			stopped++
			if stopped >= extra {
				break
			}
		}
		for i := 0; stopped < extra && i < len(containers.Items); i++ {
			id := containers.Items[len(containers.Items)-1-i].ID
			already := false
			for _, sid := range idleIDs {
				if sid == id {
					already = true
					break
				}
			}
			if already {
				continue
			}
			if _, err = a.docker.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: ToPtr(10)}); err != nil {
				continue
			}
			if _, err = a.docker.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true}); err != nil {
				continue
			}
			stopped++
			if stopped >= extra {
				break
			}
		}
	}
	a.lastScaleAt = time.Now()
	return nil
}

// reconcile 根据积压量计算目标副本并执行扩缩容。
// 启用 BacklogPerWorker 时：desired=ceil(pending/BacklogPerWorker)；否则使用阈值策略。
func (a *Autoscaler) reconcile(ctx context.Context) error {
	if !a.isAutoscalingEnabled() {
		return nil
	}
	p, err := a.pending(ctx)
	if err != nil {
		return err
	}
	cur, _, err := a.currentReplicas(ctx)
	if err != nil {
		return err
	}
	if a.cfg.BacklogPerWorker > 0 {
		desired := int((p + a.cfg.BacklogPerWorker - 1) / a.cfg.BacklogPerWorker)
		if desired < a.cfg.MinReplicas {
			desired = a.cfg.MinReplicas
		}
		if desired > a.cfg.MaxReplicas {
			desired = a.cfg.MaxReplicas
		}
		return a.scaleTo(ctx, desired)
	} else {
		if p >= a.cfg.ScaleUpThreshold && cur < a.cfg.MaxReplicas {
			return a.scaleTo(ctx, cur+a.cfg.ScaleUpStep)
		}
		if p <= a.cfg.ScaleDownThreshold && cur > a.cfg.MinReplicas {
			return a.scaleTo(ctx, cur-a.cfg.ScaleDownStep)
		}
	}
	return nil
}

// selectIdleContainers 选出最多 max 个空闲容器（其消费者 Pending=0），优先用于安全缩容。
// 通过 XINFO CONSUMERS 获取消费者 Pending，再与运行中的容器 ID 进行匹配。
func (a *Autoscaler) selectIdleContainers(ctx context.Context, max int) ([]string, error) {
	consumers, err := a.rdb.XInfoConsumers(ctx, a.cfg.StreamKey, a.cfg.GroupName).Result()
	if err != nil {
		return nil, err
	}
	filters := client.Filters{}
	filters.Add("label", "com.docker.compose.service="+a.cfg.ServiceName)
	if a.cfg.ProjectName != "" {
		filters.Add("label", "com.docker.compose.project="+a.cfg.ProjectName)
	}
	filters.Add("status", "running")
	list, err := a.docker.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
	if err != nil {
		return nil, err
	}
	runningIDs := make([]string, 0, len(list.Items))
	for _, c := range list.Items {
		runningIDs = append(runningIDs, c.ID)
	}
	var ids []string
	for _, c := range consumers {
		if c.Pending != 0 {
			continue
		}
		parts := strings.Split(c.Name, "-")
		if len(parts) == 0 {
			continue
		}
		cid := parts[0] // 消费者名中携带的容器ID前缀（hostname）
		for _, id := range runningIDs {
			if strings.HasPrefix(id, cid) {
				ids = append(ids, id)
				break
			}
		}
		if len(ids) >= max {
			break
		}
	}
	return ids, nil
}

// status 返回 autoscaler 的当前状态，用于 /status 接口展示。
func (a *Autoscaler) status(ctx context.Context) (map[string]any, error) {
	p, err := a.pending(ctx)
	if err != nil {
		return nil, err
	}
	cur, _, err := a.currentReplicas(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"pending":          p,
		"replicas":         cur,
		"service":          a.cfg.ServiceName,
		"project":          a.cfg.ProjectName,
		"autoscaling_enabled": a.isAutoscalingEnabled(),
		"last_scale_at":    a.lastScaleAt.Unix(),
		"scale_up_th":      a.cfg.ScaleUpThreshold,
		"scale_down_th":    a.cfg.ScaleDownThreshold,
		"min_replicas":     a.cfg.MinReplicas,
		"max_replicas":     a.cfg.MaxReplicas,
		"cooldown_sec":     int(a.cfg.Cooldown.Seconds()),
		"monitor_interval": int(a.cfg.MonitorInterval.Seconds()),
	}, nil
}

// main 初始化依赖、兜底创建 Stream/Group、启动调谐循环，并暴露 HTTP 接口。
func main() {
	cfg := loadConfig()
	rdb := newRedis(cfg)
	cli, err := newDockerClient()
	if err != nil {
		panic(err)
	}
	a := &Autoscaler{cfg: cfg, rdb: rdb, docker: cli, autoscalingEnabled: true}
	ctx := context.Background()
	_ = rdb.XGroupCreateMkStream(ctx, cfg.StreamKey, cfg.GroupName, "0").Err()
	go func() {
		t := time.NewTicker(cfg.MonitorInterval)
		defer t.Stop()
		for {
			<-t.C
			_ = a.reconcile(ctx)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		st, err := a.status(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b, _ := json.Marshal(st)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Replicas int `json:"replicas"`
		}
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.Replicas <= 0 {
			http.Error(w, "invalid replicas", http.StatusBadRequest)
			return
		}
		if err := a.scaleTo(ctx, body.Replicas); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("scaled"))
	})

	mux.HandleFunc("/autoscaling", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			b, _ := json.Marshal(map[string]any{"enabled": a.isAutoscalingEnabled()})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
			return
		}
		if r.Method == http.MethodPost {
			var body struct {
				Enabled bool `json:"enabled"`
			}
			dec := json.NewDecoder(r.Body)
			if err := dec.Decode(&body); err != nil {
				http.Error(w, "invalid body", http.StatusBadRequest)
				return
			}
			a.setAutoscalingEnabled(body.Enabled)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

// ToPtr 帮助函数：返回值的指针，用于构造 API 可选参数。
func ToPtr[T any](v T) *T {
	return &v
}
