# 🚀 服务器环境配置指南

## 概述

本指南提供两种服务器部署方案：简化版（推荐）和完整Docker版。

## 🎯 简化部署（推荐方案）

适用于已有Go环境，只需要Docker和ClickHouse的服务器。

### 一键配置
```bash
# 运行简化配置脚本
./setup_server_simple.sh

# 配置完成后启动应用
./start.sh
```

### 手动步骤
如果自动脚本失败，可以按以下步骤手动配置：

#### 1. 安装Docker
```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com | sh
sudo systemctl start docker
sudo usermod -aG docker $USER

# CentOS/RHEL
sudo yum install -y docker
sudo systemctl start docker
sudo usermod -aG docker $USER

# 重新登录或运行
newgrp docker
```

#### 2. 启动ClickHouse
```bash
# 使用docker-compose（推荐）
docker-compose up -d clickhouse

# 或直接运行容器
docker run -d \
  --name data4bt-clickhouse \
  -p 8123:8123 -p 9000:9000 \
  -e CLICKHOUSE_DB=data4BT \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=123456 \
  clickhouse/clickhouse-server:23.8-alpine
```

#### 3. 初始化应用
```bash
# 初始化数据库
go run cmd/main.go -cmd=init-db

# 启动应用
./start.sh
```

## 🐳 完整Docker部署

如果需要完全容器化的部署：

```bash
# 一键Docker部署
./docker-start.sh

# 或使用docker-compose
docker-compose -f docker-compose.full.yml up -d
```

详细文档：[DOCKER_DEPLOYMENT.md](DOCKER_DEPLOYMENT.md)

## 🔧 start.sh增强功能

现在的start.sh已经具备以下智能功能：

### 自动环境检测
- ✅ 检查Go环境
- ✅ 检查Docker环境（可选安装指导）
- ✅ 检查ClickHouse容器状态
- ✅ 自动启动停止的容器
- ✅ 自动创建缺失的容器

### 智能容器管理
- 🔄 自动检测并重启已停止的容器
- 🆕 自动创建不存在的ClickHouse容器
- ⏱️ 智能等待容器启动完成
- 🔍 提供详细的错误诊断信息

### 错误处理和恢复
- 📋 详细的错误提示和解决方案
- 🆘 自动安装指导
- 📊 容器日志显示
- 🔄 多种启动方式fallback

## 📊 使用方法

### 启动应用
```bash
# 前台运行（推荐用于调试）
./start.sh

# 后台运行（推荐用于生产）
./start.sh --background

# 测试模式
./start.sh --test

# 详细输出
./start.sh --verbose
```

### 停止应用
```bash
# 优雅停止
./stop.sh

# 查看状态
docker ps | grep clickhouse
```

## 🌐 外部访问

### ClickHouse数据库连接
- **HTTP接口**: `http://服务器IP:8123`
- **Native接口**: `服务器IP:9000`
- **用户名**: `default`
- **密码**: `123456`
- **数据库**: `data4BT`

### 监控面板
- **Web界面**: `http://服务器IP:8890`
- **健康检查**: `http://服务器IP:8889/health`

## 🔍 故障排除

### 常见问题

#### 1. Docker相关
```bash
# 检查Docker状态
docker info

# 启动Docker服务
sudo systemctl start docker

# 重新安装Docker
curl -fsSL https://get.docker.com | sh
```

#### 2. ClickHouse相关
```bash
# 检查容器状态
docker ps -a | grep clickhouse

# 查看容器日志
docker logs data4bt-clickhouse

# 重启容器
docker restart data4bt-clickhouse

# 重新创建容器
docker rm -f data4bt-clickhouse
docker-compose up -d clickhouse
```

#### 3. 权限问题
```bash
# 添加用户到docker组
sudo usermod -aG docker $USER
newgrp docker

# 或使用sudo运行
sudo ./start.sh
```

### 完整重置
如果遇到严重问题，可以完全重置环境：

```bash
# 停止所有容器
docker-compose down

# 清理Docker系统
docker system prune -f

# 重新启动
./setup_server_simple.sh
```

## 🎉 完成

配置完成后，你将拥有：

- ✅ 自动化的环境检测和修复
- ✅ 智能的ClickHouse容器管理
- ✅ 完善的错误处理和诊断
- ✅ 简单易用的启动停止命令
- ✅ 外部系统可访问的数据库
- ✅ Web监控界面

现在只需要运行 `./start.sh` 即可启动整个系统！