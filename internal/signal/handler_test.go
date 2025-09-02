package signal

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSignalHandler(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	timeout := 5 * time.Second

	handler := NewSignalHandler(timeout, logger)

	assert.NotNil(t, handler)
	assert.Equal(t, timeout, handler.timeout)
	assert.NotNil(t, handler.ctx)
	assert.NotNil(t, handler.cancel)
	assert.NotNil(t, handler.sigChan)
	assert.NotNil(t, handler.shutdownCh)
	assert.False(t, handler.started)
	assert.False(t, handler.stopped)
	assert.Empty(t, handler.callbacks)
}

func TestSignalHandler_Start(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 测试正常启动
	err := handler.Start(context.Background())
	assert.NoError(t, err)
	assert.True(t, handler.started)
	assert.False(t, handler.stopped)

	// 测试重复启动
	err = handler.Start(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already started")

	// 清理
	handler.Stop()
}

func TestSignalHandler_Stop(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 测试未启动时停止
	err := handler.Stop()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not started")

	// 启动后停止
	handler.Start(context.Background())
	err = handler.Stop()
	assert.NoError(t, err)
	assert.True(t, handler.stopped)

	// 测试重复停止
	err = handler.Stop()
	assert.NoError(t, err) // 重复停止应该成功
}

func TestSignalHandler_RegisterShutdownCallback(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 测试注册有效回调
	callbackCalled := false
	callback := func() {
		callbackCalled = true
	}

	err := handler.RegisterShutdownCallback(callback)
	assert.NoError(t, err)
	assert.Len(t, handler.callbacks, 1)

	// 测试注册nil回调
	err = handler.RegisterShutdownCallback(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")

	// 测试执行回调
	handler.executeCallbacks()
	assert.True(t, callbackCalled)
}

func TestSignalHandler_GetContext(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	ctx := handler.GetContext()
	assert.NotNil(t, ctx)
	assert.Equal(t, handler.ctx, ctx)
}

func TestSignalHandler_IsShutdownRequested(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 初始状态不应该请求关闭
	assert.False(t, handler.IsShutdownRequested())

	// 取消上下文后应该请求关闭
	handler.cancel()
	assert.True(t, handler.IsShutdownRequested())
}

func TestSignalHandler_Wait(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 测试上下文取消
	go func() {
		time.Sleep(100 * time.Millisecond)
		handler.cancel()
	}()

	err := handler.Wait()
	assert.Equal(t, context.Canceled, err)
}

func TestSignalHandler_GracefulShutdown(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 注册回调
	callbackCalled := false
	handler.RegisterShutdownCallback(func() {
		callbackCalled = true
	})

	// 启动处理器
	handler.Start(context.Background())

	// 模拟关闭完成
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(handler.shutdownCh)
	}()

	err := handler.GracefulShutdown(1 * time.Second)
	assert.NoError(t, err)
	assert.True(t, callbackCalled)
}

func TestSignalHandler_GracefulShutdownTimeout(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 启动处理器
	handler.Start(context.Background())

	// 不关闭shutdownCh，模拟超时
	err := handler.GracefulShutdown(100 * time.Millisecond)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")

	// 清理
	handler.Stop()
}

func TestSignalHandler_ExecuteCallbacks(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 注册多个回调
	callCount := 0
	for i := 0; i < 3; i++ {
		handler.RegisterShutdownCallback(func() {
			callCount++
		})
	}

	// 注册一个会panic的回调
	handler.RegisterShutdownCallback(func() {
		panic("test panic")
	})

	// 再注册一个正常回调
	handler.RegisterShutdownCallback(func() {
		callCount++
	})

	// 执行回调
	handler.executeCallbacks()

	// 验证正常回调都被执行了（panic的回调不会影响其他回调）
	assert.Equal(t, 4, callCount)
}

func TestSignalHandler_HandleSignals(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 启动信号处理
	handler.Start(context.Background())
	defer handler.Stop()

	// 发送SIGTERM信号
	go func() {
		time.Sleep(100 * time.Millisecond)
		handler.sigChan <- syscall.SIGTERM
	}()

	// 等待信号处理
	select {
	case <-handler.ctx.Done():
		// 信号被正确处理，上下文被取消
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Signal handling timeout")
	}

	// 验证关闭状态
	assert.True(t, handler.IsShutdownRequested())
	
	// 注意：不发送第二个信号，因为会导致os.Exit(1)
}

func TestSignalHandler_SecondSignalForceExit(t *testing.T) {
	// 这个测试比较难实现，因为它会调用os.Exit(1)
	// 在实际项目中，可以考虑将os.Exit抽象为可注入的依赖
	// 这里我们只测试第一个信号的处理
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 启动信号处理
	handler.Start(context.Background())
	defer handler.Stop()

	// 发送第一个信号
	go func() {
		time.Sleep(50 * time.Millisecond)
		handler.sigChan <- syscall.SIGINT
	}()

	// 等待第一个信号处理
	select {
	case <-handler.ctx.Done():
		// 第一个信号被正确处理
	case <-time.After(500 * time.Millisecond):
		t.Fatal("First signal handling timeout")
	}

	assert.True(t, handler.IsShutdownRequested())
	
	// 注意：我们不测试第二个信号，因为它会调用os.Exit(1)
	// 在生产代码中，可以考虑将os.Exit抽象为可测试的接口
}

func TestSignalHandler_TimeoutProtection(t *testing.T) {
	// 这个测试也比较难实现，因为timeoutProtection会调用os.Exit(1)
	// 在实际项目中，可以考虑将os.Exit抽象为可注入的依赖
	// 这里我们测试正常关闭的情况
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(100*time.Millisecond, logger)

	// 启动超时保护
	go handler.timeoutProtection()

	// 立即关闭shutdownCh，模拟正常关闭
	close(handler.shutdownCh)

	// 等待一段时间，确保没有panic或exit
	time.Sleep(200 * time.Millisecond)
	// 如果到这里没有exit，说明正常关闭路径工作正常
}

// 基准测试
func BenchmarkSignalHandler_RegisterCallback(b *testing.B) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	callback := func() {}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.RegisterShutdownCallback(callback)
	}
}

func BenchmarkSignalHandler_ExecuteCallbacks(b *testing.B) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(5*time.Second, logger)

	// 注册一些回调
	for i := 0; i < 10; i++ {
		handler.RegisterShutdownCallback(func() {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.executeCallbacks()
	}
}

// 集成测试
func TestSignalHandler_Integration(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	handler := NewSignalHandler(1*time.Second, logger)

	// 注册回调
	callbackExecuted := false
	handler.RegisterShutdownCallback(func() {
		callbackExecuted = true
	})

	// 启动处理器
	err := handler.Start(context.Background())
	require.NoError(t, err)

	// 模拟信号
	go func() {
		time.Sleep(100 * time.Millisecond)
		handler.sigChan <- syscall.SIGTERM
	}()

	// 等待关闭
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case <-handler.GetContext().Done():
		// 信号处理成功
	case <-ctx.Done():
		t.Fatal("Integration test timeout")
	}

	// 停止处理器
	err = handler.Stop()
	assert.NoError(t, err)

	// 验证回调被执行
	assert.True(t, callbackExecuted)
}