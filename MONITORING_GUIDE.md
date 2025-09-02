# 币安数据加载器监控使用指南

## 🎯 功能概述

现在你已经拥有一个完整的监控系统来查看每个币种的1分钟历史数据处理状态，包括：

1. **数据状态查询工具** - 查看每个币种的完整数据时间范围
2. **实时Web监控面板** - 浏览器中查看实时处理状态  
3. **优化的网络配置** - 解决网络连接问题
4. **详细的状态脚本** - 一键检查系统状态

## 🛠️ 快速开始

### 1. 检查系统状态
```bash
# 快速状态检查
./check_status.sh

# 详细状态查看
./tools/status_checker

# JSON格式输出
./tools/status_checker --json
```

### 2. 启动Web监控面板
```bash
# 启动监控面板 (端口8890) - 集成在主程序中
# 运行主程序时会自动启动监控面板
go run cmd/main.go

# 或使用优化启动脚本
./start_optimized.sh
```

然后在浏览器中访问：http://localhost:8890

### 3. 启动数据加载器 (优化版)
```bash
# 前台运行 (推荐用于测试)
./start_optimized.sh

# 后台运行 (推荐用于生产)
./start_optimized.sh --background

# 测试模式 (只处理BTCUSDT一周数据)
./start_optimized.sh --test
```

## 📊 监控面板功能

### 主要指标
- **总交易对数**: 系统监控的所有USDT交易对数量
- **有数据交易对**: 已有历史数据的交易对数量  
- **总记录数**: 数据库中的总1分钟K线记录数
- **数据库状态**: ClickHouse连接状态

### 详细表格
- **交易对**: 币种名称 (如 BTCUSDT)
- **记录数**: 该交易对的1分钟K线记录总数
- **最早时间**: 数据库中该交易对的最早时间
- **最新时间**: 数据库中该交易对的最新时间  
- **时间跨度**: 数据覆盖的时间范围
- **状态**: 有数据/待处理

### 自动刷新
- 监控面板每30秒自动刷新数据
- 手动刷新按钮位于右下角

## 🔧 状态查询工具

### 命令行工具
```bash
# 编译状态检查工具
go build -o tools/status_checker tools/status_checker.go

# 查看所有交易对状态 
./tools/status_checker

# JSON格式输出，便于程序处理
./tools/status_checker --json

# 快速系统状态检查
./check_status.sh
```

### 输出说明
状态工具会显示：
- 数据库连接状态
- 每个交易对的记录数量
- 实际数据的时间范围 
- 从状态文件中读取的进度信息
- 处理状态对比

## 🌐 网络优化配置

已创建优化配置文件 `config_optimized.yml`，主要改进：

### 网络设置
- **超时时间**: 增加到120秒，适应慢网络
- **重试次数**: 增加到5次，提高成功率
- **重试延迟**: 15秒，避免过于频繁重试
- **连接复用**: 启用Keep-Alive减少连接开销
- **并发控制**: 降低并发数避免网络拥堵

### 数据处理优化
- **月数据下载**: 改为按月下载，减少网络请求
- **批处理**: 增加批处理大小，减少数据库IO
- **内存管理**: 设置内存限制，避免系统卡顿
- **错误处理**: 遇到错误继续处理其他币种

## 📈 实时监控 

### Web监控面板特点
- **实时数据**: 直接连接ClickHouse数据库
- **自动刷新**: 30秒自动更新，无需手动刷新
- **响应式设计**: 支持手机、平板、电脑访问
- **中文界面**: 完全中文化的操作界面
- **状态指示**: 清晰的颜色标识不同状态

### 访问方式
1. 启动数据加载器（自动启动监控）：`go run cmd/main.go`
2. 打开浏览器访问：http://localhost:8890
3. 监控面板会在数据加载器运行时自动启用

### API接口
- **系统概览**: GET `/api/data` - 获取完整系统状态
- **健康检查**: GET `/health` - 检查服务状态

## 🚀 推荐使用流程

### 首次使用
1. **系统检查**: `./check_status.sh` 确认环境正常
2. **启动监控**: `./monitor` 开启Web面板
3. **测试运行**: `./start_optimized.sh --test` 验证功能
4. **正式运行**: `./start_optimized.sh --background` 后台执行

### 日常监控
1. **Web面板**: 浏览器访问 http://localhost:8890 查看实时状态
2. **命令检查**: `./check_status.sh` 快速检查
3. **详细状态**: `./tools/status_checker` 查看详细信息
4. **日志监控**: `tail -f logs/data_loader.log` 查看实时日志

### 问题排查
1. **网络问题**: 检查日志中的timeout错误，调整配置中的超时设置
2. **数据库问题**: 运行 `./check_status.sh` 检查ClickHouse状态
3. **进程问题**: 查看 `.data_loader_pid` 文件确认进程状态
4. **资源问题**: 监控系统内存和CPU使用情况

## 📝 文件说明

### 新增文件
- `check_status.sh` - 系统状态检查脚本
- `config_optimized.yml` - 网络优化配置文件
- `start_optimized.sh` - 优化版启动脚本
- `tools/status_checker.go` - 状态查询工具源码
- `tools/status_checker` - 编译后的状态查询工具
- `pkg/webmonitor/` - 集成的Web监控模块源码
- 监控功能已集成到主程序中，无需独立的监控工具
- `tools/monitor_dashboard.html` - 静态监控页面模板

### 配置文件
- `config_optimized.yml` - 主要用于生产环境的网络优化配置
- `config.yml` - 原始配置文件备份

## 🎯 使用建议

### 生产环境
- 使用 `./start_optimized.sh --background` 后台运行
- 定期运行 `./check_status.sh` 检查状态
- 监控 Web 面板了解实时进度
- 设置日志轮转避免日志文件过大

### 开发测试
- 使用 `./start_optimized.sh --test` 进行功能测试
- 使用 `./start_optimized.sh` 前台运行便于调试
- 通过Web面板实时观察数据变化

### 网络问题处理
- 如果频繁超时，可以进一步增加 `config_optimized.yml` 中的超时时间
- 如果网络不稳定，可以降低并发数 `max_concurrent_symbols`
- 可以调整重试策略中的参数

## ❓ 常见问题

**Q: Web监控面板显示"加载失败"？**
A: 检查ClickHouse是否运行正常，运行 `./check_status.sh` 确认数据库状态

**Q: 状态检查工具报错？**  
A: 确认Go环境正常，重新编译：`go build -o tools/status_checker tools/status_checker.go`

**Q: 数据加载器频繁超时？**
A: 使用优化配置启动：`cp config_optimized.yml config.yml && ./start.sh`

**Q: 如何查看特定交易对的处理进度？**
A: 在Web监控面板中使用表格查看，或者运行状态检查工具

**Q: 如何停止后台运行的程序？**
A: 运行 `./stop.sh` 或者直接 `kill $(cat .data_loader_pid)`

---

现在你拥有了一个功能完整的监控系统！🎉