# 开发环境配置说明

## 概述

本项目现在支持多环境配置，可以通过不同的配置文件和启动参数来适应不同的运行环境。

## 配置文件

- `config.yml` - 生产环境配置（默认）
- `config-dev.yml` - 开发环境配置

## 启动方式

### 开发环境启动

```bash
# 前台运行开发环境
./start.sh --dev

# 后台运行开发环境
./start.sh --dev --background

# 开发环境测试模式
./start.sh --dev --test --symbols BTCUSDT

# 开发环境详细输出
./start.sh --dev --verbose
```

### 生产环境启动（默认）

```bash
# 前台运行
./start.sh

# 后台运行
./start.sh --background
```

## 环境变量

当使用 `--dev` 参数时，系统会自动设置：
- `APP_ENV=development`
- 使用 `config-dev.yml` 配置文件

## 停止项目

无论使用哪种启动方式，都可以通过以下方式停止：

```bash
# 使用停止脚本
./stop.sh

# 或者在前台运行时使用 Ctrl+C
```

## 信号处理

项目支持优雅关闭：
- **Ctrl+C**: 发送 SIGINT 信号，触发优雅关闭
- **stop.sh**: 智能查找并停止所有相关进程
- **超时处理**: 如果优雅关闭超时，会强制终止进程

## 配置差异

开发环境配置 (`config-dev.yml`) 与生产环境的主要差异：
- 日志级别设置为 `debug`
- 可能包含开发专用的数据库连接
- 监控和调试功能可能更详细

## 故障排除

如果遇到启动问题：

1. 检查配置文件是否存在：
   ```bash
   ls -la config*.yml
   ```

2. 检查 Docker 环境：
   ```bash
   docker ps
   ```

3. 查看详细日志：
   ```bash
   ./start.sh --dev --verbose
   ```

4. 强制停止所有进程：
   ```bash
   ./stop.sh --verbose
   ```

## 注意事项

- 开发环境和生产环境可以使用不同的数据库配置
- 确保 ClickHouse 容器正在运行
- 开发环境配置文件不应包含生产环境的敏感信息