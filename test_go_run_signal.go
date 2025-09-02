package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// 简单的测试程序，用于验证go run的信号处理
func main() {
	fmt.Println("🧪 Go信号处理测试程序")
	fmt.Println("PID:", os.Getpid())
	fmt.Println("PPID:", os.Getppid())
	fmt.Println("按 Ctrl+C 来测试信号处理...")
	fmt.Println("")

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n✅ 收到信号: %s\n", sig.String())
		fmt.Println("🛑 正在优雅关闭...")
		cancel()

		// 设置超时强制退出
		go func() {
			time.Sleep(5 * time.Second)
			fmt.Println("⏰ 超时强制退出")
			os.Exit(1)
		}()
	}()

	// 模拟工作负载
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("📋 Context已取消，程序退出")
			return
		case t := <-ticker.C:
			fmt.Printf("⏰ 工作中... %s\n", t.Format("15:04:05"))
		}
	}
}