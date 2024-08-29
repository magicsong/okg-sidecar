package httpprobe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/magicsong/okg-sidecar/api"
	"github.com/magicsong/okg-sidecar/pkg/store"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// pluginName is the name of the plugin.
	pluginName = "http_probe"
)

type httpProber struct {
	config HttpProbeConfig
	store.StorageFactory
	status *HttpProbeStatus
	log    logr.Logger
}

// GetConfigType implements api.Plugin.
func (h *httpProber) GetConfigType() interface{} {
	return &HttpProbeConfig{}
}

// Init implements api.Plugin.
func (h *httpProber) Init(config interface{}) error {
	probeConfig, ok := config.(*HttpProbeConfig)
	if !ok {
		return fmt.Errorf("invalid config type")
	}
	h.config = *probeConfig
	h.status = &HttpProbeStatus{}
	h.log = logf.Log.WithName("http_probe")
	return nil
}

// Name implements api.Plugin.
func (h *httpProber) Name() string {
	return pluginName
}

// Start implements api.Plugin.
func (h *httpProber) Start(ctx context.Context, errorCh chan<- error) {
	h.log.Info("Starting http probe plugin")
	reloadConfig := make(chan struct{})
	if len(h.config.Endpoints) == 0 {
		h.log.Info("No endpoints to probe")
		h.status.setStatus("Stopped")
		return
	}
	var wg sync.WaitGroup
	for {
		// 为当前的一轮 goroutine 创建一个可以取消的上下文
		ctxWithCancel, cancel := context.WithCancel(ctx)
		h.status.setStatus("Running")
		// 启动所有的 probeAndStore goroutine
		for _, ep := range h.config.Endpoints {
			wg.Add(1)
			h.status.incrementGoroutines()
			go func(ec EndpointConfig) {
				defer wg.Done()
				h.probeAndStore(ctxWithCancel, errorCh, ec)
				h.status.decrementGoroutines()
			}(ep)
		}

		select {
		case <-reloadConfig:
			// 收到配置重载信号，取消当前所有的 goroutine
			fmt.Println("Received reload signal, restarting goroutines...")
			cancel()
			// 等待所有的 goroutine 退出
			wg.Wait()

			// 这里可以进行必要的配置更新操作
			// h.config = newConfig

		case <-ctx.Done():
			// 上下文被取消，退出
			h.status.setStatus("Stopped")
			cancel()
			return
		}
		wg.Wait()
		// 重启 goroutine，在下一个循环中启动新的 goroutine
	}
}

func (h *httpProber) probeAndStore(ctx context.Context, errorCh chan<- error, config EndpointConfig) {
	for {
		select {
		case <-ctx.Done():
			// 上下文被取消，安全退出
			return
		default:
			// 正常的探测和存储操作
			executor := NewExecutor(10, h.StorageFactory)
			err := executor.Probe(config)
			if err != nil {
				errorCh <- fmt.Errorf("error during probe for %v: %w", config, err)
			}
			time.Sleep(time.Second)
		}
	}
}

// Status implements api.Plugin.
func (h *httpProber) Status() (*api.PluginStatus, error) {
	return &api.PluginStatus{
		Name:    pluginName,
		Health:  h.status.getStatus(),
		Running: h.status.getStatus() == "Running",
	}, nil
}

// Stop implements api.Plugin.
func (h *httpProber) Stop(ctx context.Context) error {
	panic("unimplemented")
}

// Version implements api.Plugin.
func (h *httpProber) Version() string {
	return "v0.0.1"
}

func NewPlugin() api.Plugin {
	return &httpProber{}
}
