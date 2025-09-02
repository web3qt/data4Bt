package signal

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// ISignalHandler 信号处理接口
type ISignalHandler interface {
	// Start 开始监听信号
	Start(ctx context.Context) error

	// Stop 停止信号监听
	Stop() error

	// Wait 等待信号处理完成
	Wait() error

	// RegisterShutdownCallback 注册关闭回调函数
	RegisterShutdownCallback(callback func()) error

	// GracefulShutdown 优雅关闭
	GracefulShutdown(timeout time.Duration) error
}

// SignalHandler 统一的信号处理器
type SignalHandler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	sigChan    chan os.Signal
	shutdownCh chan struct{}
	timeout    time.Duration
	logger     zerolog.Logger
	mu         sync.RWMutex
	shutdownOnce sync.Once
	callbacks  []func()
	started    bool
	stopped    bool
}

// NewSignalHandler 创建新的信号处理器
func NewSignalHandler(timeout time.Duration, logger zerolog.Logger) *SignalHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &SignalHandler{
		ctx:        ctx,
		cancel:     cancel,
		sigChan:    make(chan os.Signal, 2), // 缓冲2个信号
		shutdownCh: make(chan struct{}),
		timeout:    timeout,
		logger:     logger,
		callbacks:  make([]func(), 0),
	}
}

// Start 开始监听信号
func (s *SignalHandler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("signal handler already started")
	}

	if s.stopped {
		return fmt.Errorf("signal handler has been stopped")
	}

	// 注册信号监听
	signal.Notify(s.sigChan, syscall.SIGINT, syscall.SIGTERM)
	s.started = true

	// 启动信号处理goroutine
	go s.handleSignals()

	s.logger.Info().Msg("Signal handler started, listening for SIGINT and SIGTERM")
	return nil
}

// Stop 停止信号监听
func (s *SignalHandler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return fmt.Errorf("signal handler not started")
	}

	if s.stopped {
		return nil // 已经停止
	}

	// 停止信号监听
	signal.Stop(s.sigChan)
	s.stopped = true

	// 取消上下文
	s.cancel()

	// 关闭shutdown channel
	close(s.shutdownCh)

	s.logger.Info().Msg("Signal handler stopped")
	return nil
}

// Wait 等待信号处理完成
func (s *SignalHandler) Wait() error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.shutdownCh:
		return nil
	}
}

// RegisterShutdownCallback 注册关闭回调函数
func (s *SignalHandler) RegisterShutdownCallback(callback func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if callback == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	s.callbacks = append(s.callbacks, callback)
	s.logger.Debug().Int("total_callbacks", len(s.callbacks)).Msg("Shutdown callback registered")
	return nil
}

// GracefulShutdown 优雅关闭
func (s *SignalHandler) GracefulShutdown(timeout time.Duration) error {
	s.logger.Info().Dur("timeout", timeout).Msg("Starting graceful shutdown")

	// 执行所有回调函数
	s.executeCallbacks()

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 等待关闭完成或超时
	select {
	case <-ctx.Done():
		s.logger.Warn().Msg("Graceful shutdown timeout")
		return fmt.Errorf("graceful shutdown timeout after %v", timeout)
	case <-s.shutdownCh:
		s.logger.Info().Msg("Graceful shutdown completed")
		return nil
	}
}

// GetContext 获取上下文
func (s *SignalHandler) GetContext() context.Context {
	return s.ctx
}

// IsShutdownRequested 检查是否请求关闭
func (s *SignalHandler) IsShutdownRequested() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

// handleSignals 处理信号的内部方法
func (s *SignalHandler) handleSignals() {
	for {
		select {
		case sig := <-s.sigChan:
			s.shutdownOnce.Do(func() {
				s.logger.Info().
					Str("signal", sig.String()).
					Msg("Received shutdown signal, initiating graceful shutdown")

				fmt.Printf("\n🛑 收到停止信号 (%s)，正在优雅关闭系统...\n", sig.String())
				fmt.Println("💡 请等待当前操作完成，系统将自动保存状态并退出")
				fmt.Println("⚡ 如果系统无响应，请再按一次 Ctrl+C 强制退出")

				// 取消上下文
				s.cancel()

				// 启动超时保护
				go s.timeoutProtection()
			})

			// 如果再次收到信号，立即强制退出
			if s.IsShutdownRequested() {
				s.logger.Warn().
					Str("signal", sig.String()).
					Msg("Received second shutdown signal, forcing immediate exit")
				fmt.Printf("\n⚠️  收到第二次停止信号 (%s)，立即强制退出！\n", sig.String())
				os.Exit(1)
			}

		case <-s.ctx.Done():
			// 上下文被取消，退出信号处理循环
			return
		}
	}
}

// executeCallbacks 执行所有回调函数
func (s *SignalHandler) executeCallbacks() {
	s.mu.RLock()
	callbacks := make([]func(), len(s.callbacks))
	copy(callbacks, s.callbacks)
	s.mu.RUnlock()

	s.logger.Info().Int("callback_count", len(callbacks)).Msg("Executing shutdown callbacks")

	for i, callback := range callbacks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error().
						Int("callback_index", i).
						Interface("panic", r).
						Msg("Shutdown callback panicked")
				}
			}()
			callback()
		}()
	}
}

// timeoutProtection 超时保护机制
func (s *SignalHandler) timeoutProtection() {
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		s.logger.Warn().Dur("timeout", s.timeout).Msg("Graceful shutdown timeout, forcing exit")
		fmt.Printf("\n⏰ 优雅关闭超时 (%v)，强制退出！\n", s.timeout)
		os.Exit(1)
	case <-s.shutdownCh:
		// 正常关闭完成
		return
	case <-s.ctx.Done():
		// 上下文取消
		return
	}
}