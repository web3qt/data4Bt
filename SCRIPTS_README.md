# Data4BT 脚本使用指南

本项目提供了一系列便捷的脚本来管理 Data4BT 系统的启动、停止和维护。

## 📋 脚本列表

### 🚀 一键启动脚本 - `start.sh`
启动完整的 Data4BT 系统，包括 ClickHouse 服务和数据库初始化。

```bash
./start.sh
```

**功能：**
- 检查 Docker 和 Go 环境
- 启动或检查 ClickHouse 容器状态
- 测试数据库连接
- 初始化数据库结构
- 显示系统状态
- 提供后续操作指引

### 🛑 一键停止脚本 - `stop.sh`
安全停止 Data4BT 系统的所有相关进程和服务。

```bash
./stop.sh
```

**功能：**
- 停止所有 Go 相关进程
- 询问是否停止 ClickHouse 容器（考虑到可能被其他项目共享）
- 清理临时文件
- 检查剩余进程
- 提供清理建议

### 🗑️ 清空数据库脚本 - `clear_database.sh`
安全清空 data4BT 数据库中的所有 K线数据。

```bash
./clear_database.sh
```

**功能：**
- 安全确认机制（需要输入 'YES'）
- 检查 ClickHouse 连接状态
- 清空所有 K线数据表
- 验证清理结果
- 提供重新加载数据的指引

**清理的数据表：**
- `klines_1m` - 1分钟K线数据
- `klines_5m` - 5分钟K线数据
- `klines_15m` - 15分钟K线数据
- `klines_1h` - 1小时K线数据
- `klines_4h` - 4小时K线数据
- `klines_1d` - 1天K线数据
- `symbol_progress` - 进度状态数据

### 📊 状态查看脚本 - `status.sh`
查看系统和数据加载状态。

```bash
# 查看所有代币状态
./status.sh

# 查看详细状态
./status.sh -d

# 查看特定代币状态
./status.sh BTCUSDT

# 查看多个代币状态
./status.sh BTCUSDT,ETHUSDT -d
```

### 🔄 ClickHouse 重启脚本 - `restart_clickhouse.sh`
专门用于重启 ClickHouse 容器。

```bash
./restart_clickhouse.sh
```

## 🔧 使用流程

### 首次启动
```bash
# 1. 启动系统
./start.sh

# 2. 查看状态
./status.sh

# 3. 开始数据加载
go run cmd/main.go -cmd=run
# 或使用并发模式
go run cmd/main.go -cmd=concurrent
```

### 日常使用
```bash
# 查看状态
./status.sh

# 更新到最新数据
go run cmd/main.go -cmd=update-latest

# 停止系统
./stop.sh
```

### 维护操作
```bash
# 清空数据库重新开始
./clear_database.sh

# 重启 ClickHouse
./restart_clickhouse.sh

# 重新初始化
./start.sh
```

## ⚠️ 注意事项

1. **ClickHouse 容器共享**
   - 根据 `docker-compose.yml`，系统使用现有的共享 ClickHouse 容器
   - 停止脚本会询问是否停止容器，避免影响其他项目

2. **数据安全**
   - 清空数据库脚本有安全确认机制
   - 需要输入 'YES' 才能执行清理操作
   - 操作不可恢复，请谨慎使用

3. **环境要求**
   - Docker（用于 ClickHouse 容器）
   - Go 1.19+（用于运行应用程序）
   - 配置文件 `configs/config.yml` 必须存在

4. **网络端口**
   - ClickHouse HTTP: `localhost:8123`
   - ClickHouse Native: `localhost:9000`
   - Web 界面: `http://localhost:8123/play`

## 🐛 故障排除

### ClickHouse 连接失败
```bash
# 检查容器状态
docker ps

# 重启 ClickHouse
./restart_clickhouse.sh

# 检查日志
docker logs <container_id>
```

### Go 进程无法停止
```bash
# 查找进程
ps aux | grep "go run"

# 手动杀死进程
kill -9 <pid>
```

### 数据库初始化失败
```bash
# 检查配置文件
cat configs/config.yml

# 检查 ClickHouse 连接
curl http://localhost:8123/ping

# 重新初始化
go run cmd/main.go -cmd=init-db
```

## 📞 获取帮助

如果遇到问题，可以：
1. 查看脚本输出的错误信息
2. 检查 ClickHouse 容器日志
3. 验证配置文件是否正确
4. 确保所有依赖服务正常运行

---

**提示：** 所有脚本都包含详细的状态输出和错误处理，请仔细阅读输出信息以了解系统状态。