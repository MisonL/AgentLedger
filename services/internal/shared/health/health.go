package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Start 启动统一健康检查 HTTP 端点，默认路径为 /healthz。
func Start(ctx context.Context, addr, service string, log *slog.Logger) <-chan error {
	mux := http.NewServeMux()
	Register(mux, service)
	return StartWithHandler(ctx, addr, log, mux)
}

// Register 将健康检查路由注册到指定 mux。
func Register(mux *http.ServeMux, service string) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": service,
			"time":    time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
}

// StartWithHandler 使用给定 handler 启动 HTTP 服务。
func StartWithHandler(ctx context.Context, addr string, log *slog.Logger, handler http.Handler) <-chan error {
	errCh := make(chan error, 1)

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		defer close(errCh)
		log.Info("health endpoint listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("health endpoint shutdown failed", "error", err)
		}
	}()

	return errCh
}
