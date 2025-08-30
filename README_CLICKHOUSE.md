# ClickHouse 部署指南

本项目现在支持完整的 ClickHouse Docker 部署方案，同时保持与现有部署方式的兼容性。

## 🚀 快速启动

### 方式一：一键启动（推荐）
```bash
# 自动检测并选择最佳启动方式
./start_clickhouse.sh

# 或使用现有的优化启动脚本（已更新支持新方案）
./start_optimized.sh
```

### 方式二：Docker Compose
```bash
# 启动 ClickHouse 服务
docker compose up -d clickhouse

# 检查服务状态
docker compose ps

# 查看日志
docker compose logs clickhouse
```

### 方式三：传统方式
如果您已有运行中的 ClickHouse 容器，脚本会自动检测并使用。

## 📋 配置信息

### 连接参数
- **地址**: localhost:9000 (Native) / localhost:8123 (HTTP)
- **用户**: default
- **密码**: 123456
- **数据库**: data4BT

### 端口映射
- `9000`: ClickHouse Native 协议端口
- `8123`: ClickHouse HTTP 接口端口

## 🔧 启动脚本说明

### `start_clickhouse.sh` - 智能启动脚本
支持三种模式：
```bash
./start_clickhouse.sh docker    # 强制使用 Docker Compose
./start_clickhouse.sh legacy    # 使用现有共享容器
./start_clickhouse.sh auto      # 自动检测（默认）
```

**特性**：
- 🔍 自动检测 Docker 环境
- 🏥 健康检查和连接测试
- 🗂️ 自动数据库初始化
- 🔧 智能容器管理
- 📊 详细状态信息

### `start_optimized.sh` - 应用启动脚本
已更新支持新的 ClickHouse 方案：
- 自动检测 `data4bt-clickhouse` 和 `shared-clickhouse` 容器
- 支持密码认证和无密码模式
- 集成网络检查和资源管理

## 🐳 Docker 服务配置

### 容器信息
- **镜像**: clickhouse/clickhouse-server:23.8-alpine
- **容器名**: data4bt-clickhouse
- **网络**: data4bt-network
- **数据持久化**: 使用 Docker volumes

### 配置文件挂载
- `./docker/clickhouse/config.xml` → ClickHouse 主配置
- `./docker/clickhouse/users.xml` → 用户配置（已设置密码）
- `./docker/clickhouse/init_database.sql` → 数据库初始化脚本

## 🗄️ 数据库管理

### 初始化数据库
```bash
# 方式一：使用应用程序初始化
go run cmd/main.go -cmd=init-db

# 方式二：直接连接数据库
docker exec data4bt-clickhouse clickhouse-client --user=default --password=123456
```

### 验证安装
```bash
# 检查数据库
docker exec data4bt-clickhouse clickhouse-client \
  --user=default --password=123456 \
  --query "SHOW DATABASES"

# 检查表结构
docker exec data4bt-clickhouse clickhouse-client \
  --user=default --password=123456 \
  --query "SHOW TABLES FROM data4BT"
```

## 🔄 容器管理

### 启动服务
```bash
docker compose up -d clickhouse
```

### 停止服务
```bash
docker compose down
```

### 重启服务
```bash
docker compose restart clickhouse
# 或使用现有脚本
./restart_clickhouse.sh
```

### 清理数据（谨慎使用）
```bash
# 停止服务并删除数据卷
docker compose down -v

# 删除所有相关容器和卷
docker compose down -v --remove-orphans
```

## 🔍 故障排除

### 检查服务状态
```bash
# 容器状态
docker ps | grep clickhouse

# 服务日志
docker compose logs clickhouse

# 健康检查
curl -s http://localhost:8123/ping
```

### 常见问题

#### 1. 连接被拒绝
```bash
# 检查容器是否运行
docker ps | grep clickhouse

# 检查端口占用
lsof -i :9000
lsof -i :8123
```

#### 2. 认证失败
```bash
# 检查密码配置
docker exec data4bt-clickhouse cat /etc/clickhouse-server/users.xml | grep -A5 -B5 password
```

#### 3. 数据库不存在
```bash
# 手动创建数据库
docker exec data4bt-clickhouse clickhouse-client \
  --user=default --password=123456 \
  --query "CREATE DATABASE IF NOT EXISTS data4BT"
```

## 🔧 高级配置

### 自定义配置
编辑配置文件后重启服务：
- `docker/clickhouse/config.xml` - 主配置
- `docker/clickhouse/users.xml` - 用户配置

```bash
docker compose restart clickhouse
```

### 性能调优
修改 `config.xml` 中的参数：
- `max_connections`: 最大连接数
- `max_concurrent_queries`: 最大并发查询数
- `max_memory_usage`: 内存使用限制

### 数据备份
```bash
# 备份数据库
docker exec data4bt-clickhouse clickhouse-client \
  --user=default --password=123456 \
  --query "BACKUP DATABASE data4BT TO File('/var/lib/clickhouse/backup/')"
```

## 📚 相关文档

- [ClickHouse 官方文档](https://clickhouse.com/docs/)
- [Docker Compose 文档](https://docs.docker.com/compose/)
- 项目相关文档：
  - `README.md` - 项目主文档
  - `STATUS_GUIDE.md` - 状态监控指南
  - `MONITORING_GUIDE.md` - 监控指南

## ❗ 重要提醒

1. **密码安全**: 生产环境请修改默认密码 `123456`
2. **数据持久化**: Docker 卷确保数据不会因容器重启而丢失
3. **端口冲突**: 确保 8123 和 9000 端口未被占用
4. **网络安全**: 根据需要配置防火墙规则
5. **备份策略**: 建立定期备份机制保护重要数据