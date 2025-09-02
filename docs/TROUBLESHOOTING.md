# Data4BT 故障排除指南

本文档提供了Data4BT项目常见问题的诊断和解决方案。

## 目录

- [信号处理问题](#信号处理问题)
- [进程管理问题](#进程管理问题)
- [脚本执行问题](#脚本执行问题)
- [数据库连接问题](#数据库连接问题)
- [下载和网络问题](#下载和网络问题)
- [性能问题](#性能问题)
- [数据质量问题](#数据质量问题)
- [环境配置问题](#环境配置问题)
- [诊断工具](#诊断工具)

## 信号处理问题

### 问题1: Ctrl+C无法停止程序

**症状**: 按下Ctrl+C后程序没有响应，或者响应很慢

**原因分析**:
- 程序可能在执行阻塞操作
- 信号处理器没有正确设置
- 存在僵尸进程或孤儿进程

**解决方案**:

1. **使用改进的停止脚本** (推荐)
   ```bash
   # 智能停止所有相关进程
   ./stop.sh
   
   # 查看详细停止过程
   ./stop.sh --verbose
   
   # 预览将要停止的进程
   ./stop.sh --dry-run
   ```

2. **手动查找和终止进程**
   ```bash
   # 查找Data4BT相关进程
   pgrep -f "go run.*cmd/main.go"
   pgrep -f "data4bt"
   
   # 优雅终止
   pkill -TERM -f "go run.*cmd/main.go"
   
   # 强制终止（如果优雅终止失败）
   pkill -KILL -f "go run.*cmd/main.go"
   ```

3. **清理残留文件**
   ```bash
   # 删除PID文件
   rm -f .data_loader_pid
   
   # 清理临时文件
   rm -f /tmp/data4bt_*
   ```

### 问题2: 程序启动后立即退出

**症状**: 程序启动后没有错误信息就退出了

**诊断步骤**:

1. **检查是否有残留进程**
   ```bash
   ./stop.sh --dry-run
   ```

2. **使用测试模式启动**
   ```bash
   ./start.sh --test --verbose
   ```

3. **检查日志文件**
   ```bash
   tail -f logs/app.log
   ```

## 进程管理问题

### 问题3: 多个实例同时运行

**症状**: 系统资源占用过高，或者出现数据冲突

**诊断**:
```bash
# 查看所有相关进程
./stop.sh --dry-run --verbose

# 或者手动查看
ps aux | grep -E "(data4bt|go run.*cmd/main.go)"
```

**解决方案**:
```bash
# 停止所有实例
./stop.sh --verbose

# 确认清理完成
./stop.sh --dry-run

# 重新启动单个实例
./start.sh
```

### 问题4: 进程变成僵尸进程

**症状**: 进程显示为`<defunct>`状态

**诊断**:
```bash
# 查看僵尸进程
ps aux | grep defunct

# 查看进程树
pstree -p
```

**解决方案**:
```bash
# 重启父进程通常可以清理僵尸进程
./stop.sh --force-timeout 5
./start.sh
```

## 脚本执行问题

### 问题5: 脚本权限错误

**症状**: `Permission denied` 错误

**解决方案**:
```bash
# 添加执行权限
chmod +x start.sh stop.sh
chmod +x scripts/*.sh

# 验证权限
ls -la *.sh scripts/*.sh
```

### 问题6: 脚本依赖缺失

**症状**: `No such file or directory` 错误

**诊断**:
```bash
# 检查脚本依赖
ls -la scripts/

# 检查函数库
test -f scripts/process_manager.sh && echo "存在" || echo "缺失"
test -f scripts/start_functions.sh && echo "存在" || echo "缺失"
```

**解决方案**:
如果缺失关键文件，请从项目仓库重新获取或联系维护人员。

### 问题7: 配置文件问题

**症状**: 配置相关错误

**诊断**:
```bash
# 检查配置文件
test -f config.yml && echo "配置文件存在" || echo "配置文件缺失"

# 验证配置格式
go run cmd/main.go -cmd=validate-config 2>&1 | head -10
```

**解决方案**:
```bash
# 复制示例配置
cp configs/config.yml.example config.yml

# 或者从configs目录复制
cp configs/config.yml .
```

## 数据库连接问题

### 问题8: ClickHouse连接失败

**症状**: `Authentication failed` 或连接超时错误

**诊断步骤**:

1. **检查ClickHouse服务状态**
   ```bash
   # 检查Docker容器
   docker ps | grep clickhouse
   
   # 检查端口监听
   netstat -tlnp | grep 9000
   lsof -i :9000
   ```

2. **测试连接**
   ```bash
   # HTTP接口测试
   curl http://localhost:8123/ping
   
   # 原生协议测试
   telnet localhost 9000
   ```

**解决方案**:

1. **重启ClickHouse容器**
   ```bash
   # 使用提供的重启脚本
   ./restart_clickhouse.sh
   
   # 或者手动重启
   docker compose down clickhouse
   docker compose up -d clickhouse
   
   # 等待启动完成
   sleep 30
   curl http://localhost:8123/ping
   ```

2. **检查配置**
   ```bash
   # 验证数据库配置
   grep -A 10 "database:" config.yml
   ```

### 问题9: 数据库初始化失败

**症状**: 表创建失败或权限错误

**解决方案**:
```bash
# 重新初始化数据库
go run cmd/main.go -cmd=init-db

# 检查表是否创建成功
go run cmd/main.go -cmd=list-symbols
```

## 下载和网络问题

### 问题10: 下载速度慢或失败

**症状**: 下载超时或速度异常慢

**诊断**:
```bash
# 测试网络连接
curl -I https://data.binance.vision/

# 检查DNS解析
nslookup data.binance.vision
```

**解决方案**:

1. **调整并发设置**
   ```yaml
   # 在config.yml中降低并发数
   downloader:
     concurrency: 2  # 降低并发数
     timeout: 60s    # 增加超时时间
   ```

2. **使用代理** (如果需要)
   ```bash
   export HTTP_PROXY=http://proxy:port
   export HTTPS_PROXY=http://proxy:port
   ```

### 问题11: 数据验证失败

**症状**: CSV解析错误或数据格式问题

**诊断**:
```bash
# 检查下载的文件
ls -la data/

# 查看文件内容
head -5 data/BTCUSDT-1m-2024-01.csv
```

**解决方案**:
```bash
# 重新下载有问题的文件
rm data/BTCUSDT-1m-2024-01.csv
go run cmd/main.go -cmd=run -symbols=BTCUSDT
```

## 性能问题

### 问题12: 内存使用过高

**症状**: 系统内存不足或OOM错误

**诊断**:
```bash
# 监控内存使用
top -p $(pgrep -f "go run.*cmd/main.go")

# 查看Go程序内存统计
go tool pprof http://localhost:8890/debug/pprof/heap
```

**解决方案**:

1. **调整配置参数**
   ```yaml
   # 在config.yml中调整
   downloader:
     concurrency: 3      # 降低并发数
     buffer_size: 512    # 减少缓冲区
   
   importer:
     batch_size: 500     # 减少批次大小
     buffer_size: 5000   # 减少缓冲区
   ```

2. **增加刷新频率**
   ```yaml
   importer:
     flush_interval: "10s"  # 更频繁地刷新
   ```

### 问题13: CPU使用率过高

**解决方案**:
```yaml
# 降低并发处理
downloader:
  concurrency: 2

scheduler:
  concurrent_symbols: 2
```

## 数据质量问题

### 问题14: 数据缺失或不完整

**诊断**:
```bash
# 检查数据范围
go run cmd/main.go -cmd=range-query -symbols=BTCUSDT

# 验证数据完整性
go run cmd/main.go -cmd=validate -symbols=BTCUSDT
```

**解决方案**:
```bash
# 重新下载缺失的数据
go run cmd/main.go -cmd=update-latest -symbols=BTCUSDT
```

## 环境配置问题

### 问题15: Go版本不兼容

**诊断**:
```bash
# 检查Go版本
go version

# 检查模块要求
grep "go " go.mod
```

**解决方案**:
确保使用Go 1.19或更高版本。

### 问题16: 依赖包问题

**解决方案**:
```bash
# 清理并重新下载依赖
go clean -modcache
go mod download
go mod tidy
```

## 诊断工具

### 快速诊断脚本

创建一个快速诊断脚本 `diagnose.sh`:

```bash
#!/bin/bash
echo "=== Data4BT 系统诊断 ==="
echo

echo "1. 检查运行中的进程:"
./stop.sh --dry-run
echo

echo "2. 检查脚本权限:"
ls -la *.sh scripts/*.sh
echo

echo "3. 检查配置文件:"
test -f config.yml && echo "✓ config.yml 存在" || echo "✗ config.yml 缺失"
echo

echo "4. 检查ClickHouse:"
curl -s http://localhost:8123/ping && echo "✓ ClickHouse 响应正常" || echo "✗ ClickHouse 无响应"
echo

echo "5. 检查Go环境:"
go version
echo

echo "6. 检查网络连接:"
curl -s -I https://data.binance.vision/ | head -1
echo

echo "诊断完成"
```

### 日志分析

```bash
# 查看错误日志
grep -i error logs/app.log | tail -10

# 查看最近的活动
tail -50 logs/app.log

# 查看特定时间段的日志
grep "2024-01-15" logs/app.log
```

### 性能监控

```bash
# 监控系统资源
htop

# 监控网络连接
netstat -an | grep :8890

# 监控磁盘使用
df -h
du -sh data/
```

## 获取帮助

如果以上解决方案都无法解决问题，请：

1. 收集诊断信息：
   ```bash
   # 运行诊断脚本
   ./diagnose.sh > diagnosis.txt
   
   # 收集日志
   tail -100 logs/app.log > recent_logs.txt
   
   # 收集系统信息
   uname -a > system_info.txt
   go version >> system_info.txt
   docker version >> system_info.txt
   ```

2. 提供详细的错误描述和重现步骤

3. 联系项目维护人员或提交Issue

## 预防措施

1. **定期检查**
   ```bash
   # 每日检查脚本
   ./stop.sh --dry-run  # 检查是否有残留进程
   go run cmd/main.go -cmd=status  # 检查系统状态
   ```

2. **监控资源使用**
   - 设置内存和CPU使用率告警
   - 监控磁盘空间
   - 定期检查日志文件大小

3. **备份重要配置**
   ```bash
   # 备份配置文件
   cp config.yml config.yml.backup
   ```

4. **保持更新**
   - 定期更新项目代码
   - 关注更新日志中的重要变更
   - 测试新版本功能