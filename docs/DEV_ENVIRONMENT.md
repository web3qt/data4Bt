# 开发环境配置指南

本文档描述了如何在不同环境中配置和启动数据加载器项目。

## 环境概述

项目支持两种运行环境：

- **开发环境 (Development)**: 用于本地开发和调试
- **生产环境 (Production)**: 用于生产部署，优化性能和稳定性

## 配置文件

### 开发环境配置

**配置文件**: `configs/config-dev.yml`

开发环境特点：
- 详细的调试日志 (debug级别)
- 较低的并发数，便于调试
- 启用代理支持
- 较短的超时时间
- 显示启动概览

### 生产环境配置

**配置文件**: `configs/config-prod.yml`

生产环境特点：
- 优化的日志级别 (warn级别)
- 高并发处理能力
- 大批次数据处理
- 完整的监控和告警
- 高可用性配置
- 性能优化参数

## 启动方式

### 开发环境启动

```bash
# 前台运行 (推荐用于开发调试)
./start.sh --dev

# 后台运行
./start.sh --dev --background

# 详细输出模式
./start.sh --dev --verbose
```

### 生产环境启动

```bash
# 前台运行
./start.sh --prod

# 后台运行 (推荐用于生产部署)
./start.sh --prod --background

# 详细输出模式
./start.sh --prod --verbose
```

### 默认启动 (使用config.yml)

```bash
# 前台运行
./start.sh

# 后台运行
./start.sh --background
```

## 停止服务

无论在哪种环境下，都可以使用以下方式停止服务：

```bash
# 使用停止脚本
./stop.sh

# 或者使用 Ctrl+C (前台运行时)
```

## 环境变量

### 开发环境

开发环境会自动设置：
```bash
export APP_ENV=development
```

### 生产环境

生产环境会自动设置：
```bash
export APP_ENV=production
```

### 生产环境必需的环境变量

生产环境需要设置以下敏感信息：

```bash
# 数据库连接信息
export BDL_DATABASE_USERNAME=prod_user
export BDL_DATABASE_PASSWORD=secure_password_here

# 可选的性能调优参数
export BDL_LOG_LEVEL=warn
export BDL_DOWNLOADER_CONCURRENCY=20
export BDL_PARSER_CONCURRENCY=10
export BDL_IMPORTER_BATCH_SIZE=50000
```

## 配置差异对比

| 配置项 | 开发环境 | 生产环境 |
|--------|----------|----------|
| 日志级别 | debug | warn |
| 日志格式 | text | json |
| 下载并发数 | 5 | 20 |
| 解析并发数 | 3 | 10 |
| 批次大小 | 10000 | 50000 |
| 超时时间 | 30s | 120s |
| 重试次数 | 3 | 10 |
| 监控端口 | 8080 | 8080 |
| Web面板端口 | 8890 | 8890 |
| 启动概览 | 显示 | 隐藏 |

## 部署建议

### 开发环境

1. 使用前台运行模式便于调试
2. 启用详细日志输出
3. 使用较小的数据集进行测试
4. 定期检查配置文件更新

### 生产环境

1. **安全性**:
   - 使用环境变量管理敏感信息
   - 定期更新密码和密钥
   - 限制网络访问权限

2. **性能优化**:
   - 根据服务器规格调整并发参数
   - 监控系统资源使用情况
   - 配置适当的内存和CPU限制

3. **监控和告警**:
   - 启用完整的监控功能
   - 配置日志聚合和分析
   - 设置关键指标告警

4. **高可用性**:
   - 配置多个数据库节点
   - 实施数据备份策略
   - 配置故障转移机制

5. **运维管理**:
   - 使用后台运行模式
   - 配置自动重启机制
   - 定期检查服务状态

## 故障排除

### 常见问题

1. **配置文件不存在**:
   ```bash
   # 复制示例配置文件
   cp configs/config-dev.yml.example configs/config-dev.yml
   cp configs/config-prod.yml.example configs/config-prod.yml
   ```

2. **环境变量未设置**:
   ```bash
   # 检查环境变量
   echo $APP_ENV
   echo $BDL_DATABASE_PASSWORD
   ```

3. **端口冲突**:
   - 检查端口是否被占用
   - 修改配置文件中的端口设置

4. **权限问题**:
   ```bash
   # 确保脚本有执行权限
   chmod +x start.sh stop.sh
   ```

### 日志查看

```bash
# 开发环境日志
tail -f logs/dev.log

# 生产环境日志
tail -f logs/prod.log

# 实时监控
./start.sh --dev --verbose
```

## 配置文件管理

### 版本控制

- 配置示例文件 (`.example`) 应该提交到版本控制
- 实际配置文件 (`config-dev.yml`, `config-prod.yml`) 包含敏感信息，不应提交
- 使用 `.gitignore` 排除敏感配置文件

### 配置同步

```bash
# 从示例文件创建配置
cp configs/config-dev.yml.example configs/config-dev.yml
cp configs/config-prod.yml.example configs/config-prod.yml

# 根据实际环境修改配置
vim configs/config-dev.yml
vim configs/config-prod.yml
```

## 性能调优

### 开发环境调优

- 降低并发数以便调试
- 使用较小的批次大小
- 启用详细日志

### 生产环境调优

- 根据服务器规格调整并发参数
- 优化批次大小和缓冲区设置
- 配置适当的超时和重试策略
- 监控资源使用情况并调整参数

## 更新日志

- **v1.0.0**: 初始版本，支持基本的开发和生产环境配置
- **v1.1.0**: 添加多环境配置支持，移除测试环境
- **v1.2.0**: 优化生产环境配置，增强安全性和性能