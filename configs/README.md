# 配置文件说明

本目录包含不同环境的配置文件模板，支持多环境部署和配置管理。

## 配置文件结构

```
configs/
├── README.md                 # 本文档
├── config-dev.yml.example    # 开发环境配置模板
├── config-test.yml.example   # 测试环境配置模板
└── config-prod.yml.example   # 生产环境配置模板
```

## 快速开始

### 1. 选择环境配置

根据你的运行环境，复制对应的配置模板：

```bash
# 开发环境
cp configs/config-dev.yml.example config-dev.yml

# 测试环境
cp configs/config-test.yml.example config-test.yml

# 生产环境
cp configs/config-prod.yml.example config-prod.yml
```

### 2. 编辑配置文件

根据你的实际环境修改配置文件中的参数，特别注意：

- **数据库连接信息**：修改ClickHouse的主机、端口、用户名、密码
- **并发参数**：根据服务器性能调整并发数
- **日志配置**：设置合适的日志级别和输出路径
- **监控端口**：确保端口不冲突

### 3. 使用配置文件

有多种方式指定配置文件：

#### 方式1：环境变量（推荐）
```bash
# 设置环境
export BDL_ENV=dev
# 运行程序，自动使用config-dev.yml
go run cmd/main.go -cmd=run
```

#### 方式2：命令行参数
```bash
# 直接指定配置文件
go run cmd/main.go -config=config-prod.yml -cmd=run

# 或指定环境
go run cmd/main.go -env=prod -cmd=run
```

#### 方式3：默认配置
```bash
# 复制为默认配置文件
cp config-dev.yml config.yml
# 运行程序
go run cmd/main.go -cmd=run
```

## 环境配置详解

### 开发环境 (config-dev.yml)

**特点**：
- 低并发，减少对开发机器的压力
- 详细日志，便于调试
- 短刷新间隔，便于测试
- 启用所有验证和检查

**适用场景**：
- 本地开发和调试
- 功能测试
- 代码验证

### 测试环境 (config-test.yml)

**特点**：
- 中等并发，平衡性能和稳定性
- 结构化日志，便于自动化分析
- 固定测试数据，保证可重现性
- 禁用Web面板，避免干扰测试

**适用场景**：
- 自动化测试
- 集成测试
- 性能基准测试
- CI/CD流水线

### 生产环境 (config-prod.yml)

**特点**：
- 高并发，最大化性能
- 优化的日志级别，减少I/O开销
- 完整的监控和告警
- 高可用性配置

**适用场景**：
- 生产部署
- 大规模数据处理
- 7x24小时运行

## 配置优先级

系统按以下优先级加载配置：

1. **命令行指定的配置文件** (`-config=file.yml`) - 最高优先级
2. **环境特定配置文件** (`config-{env}.yml`)
3. **默认配置文件** (`config.yml`)
4. **内置默认值** - 最低优先级

环境变量可以覆盖任何配置项，格式为 `BDL_SECTION_KEY`，例如：
```bash
export BDL_DATABASE_CLICKHOUSE_PASSWORD=new_password
export BDL_LOG_LEVEL=debug
```

## 环境检测

系统支持多种环境检测方式：

### 1. 命令行参数（优先级最高）
```bash
go run cmd/main.go -env=prod -cmd=run
```

### 2. 环境变量
```bash
export BDL_ENV=prod
go run cmd/main.go -cmd=run
```

### 3. 配置文件存在性检测
系统会自动检测当前目录下是否存在环境特定的配置文件

### 4. 运行环境特征检测
- 容器环境 → 生产环境
- CI/CD环境 → 测试环境
- 开发目录特征 → 开发环境

## 配置验证

启动时系统会验证配置的有效性：

- **必需字段检查**：确保关键配置项不为空
- **数值范围检查**：确保并发数、超时时间等在合理范围内
- **文件路径检查**：确保日志路径、状态文件路径可写
- **网络连接检查**：验证数据库连接配置

## 安全最佳实践

### 1. 敏感信息管理

**不要在配置文件中硬编码敏感信息**，使用环境变量：

```yaml
# ❌ 错误做法
database:
  clickhouse:
    password: "hardcoded_password"

# ✅ 正确做法
database:
  clickhouse:
    password: "${BDL_DATABASE_PASSWORD}"
```

### 2. 文件权限

```bash
# 设置配置文件权限，只有所有者可读写
chmod 600 config-*.yml
```

### 3. 版本控制

```bash
# .gitignore 中应包含实际配置文件
config.yml
config-*.yml

# 只提交模板文件
config-*.yml.example
```

## 故障排除

### 配置文件未找到

```bash
# 检查当前目录
ls -la config*.yml

# 检查环境变量
echo $BDL_ENV

# 使用调试模式查看配置加载过程
export BDL_LOG_LEVEL=debug
go run cmd/main.go -cmd=run
```

### 配置验证失败

```bash
# 检查配置文件语法
yamlint config-dev.yml

# 检查必需字段
grep -E "(database|clickhouse|hosts)" config-dev.yml
```

### 环境变量覆盖不生效

```bash
# 检查环境变量格式
env | grep BDL_

# 正确的环境变量格式示例
export BDL_DATABASE_CLICKHOUSE_PASSWORD=newpass
export BDL_LOG_LEVEL=debug
```

## 配置模板定制

如果需要创建自定义环境配置：

1. 复制最接近的环境模板
2. 修改配置参数
3. 更新环境检测逻辑（如需要）
4. 添加相应的文档说明

## 相关文档

- [项目README](../README.md) - 项目总体说明
- [使用指南](../USAGE_GUIDE.md) - 详细使用说明
- [多环境配置指南](../docs/MULTI_ENV_CONFIG.md) - 多环境配置详细说明
- [故障排除指南](../docs/TROUBLESHOOTING.md) - 常见问题解决方案

## 支持

如果遇到配置相关问题：

1. 查看本文档的故障排除部分
2. 检查项目的故障排除指南
3. 查看项目Issues
4. 提交新的Issue并附上配置文件和错误信息