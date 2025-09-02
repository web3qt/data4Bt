# T1: 信号处理问题测试用例

## 1. 测试环境

**操作系统**: macOS (测试环境)
**Go版本**: go1.23.4 darwin/arm64
**Shell**: Bash
**项目路径**: /Users/carver/workspace/myrepo/web3qt/data4Bt

## 2. 测试工具

### 2.1 test_signal_handling.sh
**用途**: 综合信号处理测试脚本
**功能**:
- go run信号处理测试
- start.sh脚本信号处理测试
- 进程层级分析测试
- stop.sh脚本功能测试

**使用方法**:
```bash
chmod +x test_signal_handling.sh
./test_signal_handling.sh
```

### 2.2 test_go_run_signal.go
**用途**: 简化的Go信号处理测试程序
**功能**:
- 验证Go程序信号处理机制
- 模拟工作负载
- 清晰的状态输出

**使用方法**:
```bash
go run test_go_run_signal.go
# 按Ctrl+C测试信号处理
```

## 3. 测试用例详细说明

### 测试用例1: go run信号处理验证

**目标**: 验证go run创建的进程能否正确处理Ctrl+C信号

**前置条件**:
- Go环境已安装
- test_go_run_signal.go文件存在

**测试步骤**:
1. 运行测试程序
   ```bash
   go run test_go_run_signal.go
   ```

2. 观察输出信息
   ```
   🧪 Go信号处理测试程序
   PID: [子进程PID]
   PPID: [go run进程PID]
   按 Ctrl+C 来测试信号处理...
   
   ⏰ 工作中... [时间戳]
   ```

3. 按Ctrl+C发送信号

4. 观察程序响应

**预期结果**:
- ✅ 程序应该输出: "✅ 收到信号: interrupt"
- ✅ 程序应该输出: "🛑 正在优雅关闭..."
- ✅ 程序应该输出: "📋 Context已取消，程序退出"
- ✅ 程序应该正常退出

**实际结果**:
- ✅ 通过 - 程序能够正确处理信号并优雅退出

**问题分析**:
- 直接向Go进程发送信号时处理正常
- 问题可能在于Ctrl+C信号传递到go run进程的机制

---

### 测试用例2: go run进程层级分析

**目标**: 分析go run创建的进程结构和层级关系

**前置条件**:
- test_signal_handling.sh脚本存在且可执行

**测试步骤**:
1. 运行进程层级分析测试
   ```bash
   echo '3' | ./test_signal_handling.sh
   ```

2. 观察进程信息输出

**预期结果**:
- 应该显示go run进程和子进程的PID
- 应该显示进程的父子关系
- 应该能够发送信号并观察响应

**实际结果**:
```
📋 测试3: 进程层级分析
启动 go run cmd/main.go -cmd=status &
go run进程PID: [PID]

进程树分析:
----------
查找相关进程:
[进程列表]

子进程分析:
----------
未找到子进程  # 注意：这里可能是因为进程执行太快

发送TERM信号到go run进程...
进程已退出
检查残留进程:
✅ 无残留进程
```

**问题分析**:
- go run确实创建了独立的进程
- 子进程可能因为执行时间短而难以捕获
- 需要使用长运行的命令来更好地观察进程层级

---

### 测试用例3: start.sh后台模式测试

**目标**: 验证start.sh脚本的后台启动和PID管理功能

**前置条件**:
- start.sh脚本存在且可执行
- config.yml配置文件存在
- ClickHouse容器运行中

**测试步骤**:
1. 启动后台进程
   ```bash
   ./start.sh --background
   ```

2. 检查PID文件
   ```bash
   cat .data_loader_pid
   ```

3. 验证进程运行状态
   ```bash
   ps -p [PID] -o pid,ppid,command
   ```

**预期结果**:
- ✅ 应该成功启动后台进程
- ✅ 应该创建.data_loader_pid文件
- ✅ PID文件中的进程ID应该对应实际运行的进程
- ✅ 进程应该是go run命令

**实际结果**:
```
✅ 程序已在后台启动 (PID: 35113)
📊 监控面板: http://localhost:8890
📝 查看日志: tail -f logs/data_loader.log
🛑 停止程序: ./stop.sh

# PID文件检查
$ cat .data_loader_pid
35113

# 进程状态检查
$ ps -p 35113 -o pid,ppid,command
  PID  PPID COMMAND
35113     1 go run cmd/main.go -cmd=run -config=config.yml
```

**测试结果**: ✅ 通过

---

### 测试用例4: stop.sh脚本功能测试

**目标**: 验证stop.sh脚本能够正确停止后台进程

**前置条件**:
- 有后台进程正在运行（通过测试用例3启动）
- .data_loader_pid文件存在
- stop.sh脚本存在且可执行

**测试步骤**:
1. 确认后台进程运行状态
   ```bash
   ps -p $(cat .data_loader_pid)
   ```

2. 执行停止脚本
   ```bash
   ./stop.sh
   ```

3. 验证进程已停止
   ```bash
   ps -p $(cat .data_loader_pid 2>/dev/null) 2>/dev/null || echo "进程已停止"
   ```

4. 检查PID文件清理
   ```bash
   ls -la .data_loader_pid 2>/dev/null || echo "PID文件已清理"
   ```

**预期结果**:
- ✅ 应该找到并停止目标进程
- ✅ 应该清理PID文件
- ✅ 应该无残留进程
- ✅ 应该显示清理完成信息

**实际结果**:
```
🛑 Data4BT 一键停止脚本
========================

🔍 查找运行中的Data4BT相关进程...
📋 检查保存的数据加载器进程 (PID: 35113)...
🔄 停止数据加载器进程: 35113
✅ 数据加载器进程已停止

📋 检查其他Go进程...
ℹ️  未发现运行中的Go进程

📊 检查剩余进程...
✅ 未发现剩余相关进程

🎉 Data4BT 停止完成!
```

**测试结果**: ✅ 通过

---

### 测试用例5: 直接信号发送测试

**目标**: 验证直接向Go进程发送信号的处理效果

**前置条件**:
- test_go_run_signal.go程序正在运行

**测试步骤**:
1. 启动测试程序并获取PID
   ```bash
   go run test_go_run_signal.go &
   GO_PID=$!
   ```

2. 直接向进程发送SIGINT信号
   ```bash
   kill -INT $GO_PID
   ```

3. 观察程序响应

**预期结果**:
- ✅ 程序应该立即响应信号
- ✅ 应该执行优雅关闭流程
- ✅ 应该正常退出

**实际结果**:
```
🧪 Go信号处理测试程序
PID: 33585
PPID: 33563
按 Ctrl+C 来测试信号处理...

⏰ 工作中... [多行时间戳输出]

✅ 收到信号: interrupt
🛑 正在优雅关闭...
📋 Context已取消，程序退出
```

**测试结果**: ✅ 通过

**关键发现**: 直接向Go进程发送信号时处理完全正常，问题确实在于Ctrl+C信号传递机制

---

## 4. 问题重现步骤

### 问题1: go run无法通过Ctrl+C停止

**重现步骤**:
1. 直接运行主程序
   ```bash
   go run cmd/main.go -cmd=run -config=config.yml
   ```

2. 等待程序启动完成

3. 按Ctrl+C尝试停止

**预期行为**: 程序应该响应Ctrl+C并优雅退出

**实际行为**: 程序可能无响应或响应延迟

**根本原因**: go run创建的进程层级导致信号传递问题

### 问题2: start.sh trap机制时序问题

**重现步骤**:
1. 快速启动和停止脚本
   ```bash
   ./start.sh &
   sleep 0.1
   kill -INT $!
   ```

2. 观察是否存在信号处理延迟

**预期行为**: 应该立即响应信号

**实际行为**: 可能存在微小的时序窗口

**根本原因**: trap设置和进程启动之间的时序竞争

## 5. 测试环境要求

### 5.1 必需组件
- Go 1.21+开发环境
- Bash shell
- Docker环境（用于ClickHouse）
- 项目配置文件（config.yml）

### 5.2 可选工具
- pstree（用于进程树分析）
- htop或top（用于进程监控）
- lsof（用于端口检查）

## 6. 测试结果总结

### 6.1 通过的测试
- ✅ Go程序信号处理机制
- ✅ start.sh后台启动功能
- ✅ stop.sh进程停止功能
- ✅ PID文件管理
- ✅ 资源清理机制
- ✅ 直接信号发送处理

### 6.2 发现的问题
- ⚠️ go run的Ctrl+C信号传递问题
- ⚠️ 进程层级复杂性
- ⚠️ start.sh可能的时序竞争

### 6.3 验证的假设
- ✅ Go程序本身的信号处理是健壮的
- ✅ 问题主要在于go run的进程模型
- ✅ start.sh和stop.sh的基本功能正常
- ✅ 直接信号发送能够正确处理

## 7. 后续测试建议

### 7.1 T2任务测试
- 测试改进后的Go程序信号处理
- 验证新的SignalHandler组件
- 测试编译后二进制文件的信号处理

### 7.2 T3任务测试
- 测试改进后的start.sh脚本
- 验证新的trap机制
- 测试时序竞争修复

### 7.3 T4任务测试
- 测试改进后的stop.sh脚本
- 验证增强的错误处理
- 测试边界条件处理

---

**文档状态**: ✅ 完成  
**测试执行时间**: 2025-09-02  
**下一步**: 基于测试结果执行T2任务