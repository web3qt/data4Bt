# Binance Data Loader 项目概述

## 项目目的
一个高性能的币安K线数据下载和处理系统，支持从币安官方数据源下载历史K线数据，解析CSV文件，验证数据完整性，并将数据导入到ClickHouse数据库中。

## 技术栈
- **语言**: Go 1.19+
- **数据库**: ClickHouse 21.8+
- **容器化**: Docker & Docker Compose
- **配置**: YAML配置文件

## 主要功能
- 高性能数据下载（并发下载，自动重试）
- ClickHouse数据存储
- 多时间周期物化视图（5m, 15m, 1h, 4h, 1d）
- 实时进度监控和Web仪表板
- CSV数据导出功能
- 数据完整性验证

## 项目结构
- `cmd/` - 应用程序入口
- `internal/` - 内部包（配置、日志、状态管理等）  
- `pkg/` - 公共包（币安下载、ClickHouse存储、数据导入等）
- `configs/` - 配置文件
- `scripts/` - 脚本文件
- `test/` - 测试文件