# Align阶段：verify-data命令问题分析与需求明确

## 项目上下文分析

### 现有项目结构
- **技术栈**: Go + ClickHouse + 币安API
- **架构模式**: 分层架构（domain, repository, downloader, quality checker）
- **数据模型**: klines_1m表存储1分钟K线数据，symbol_infos表存储交易对元信息
- **现有组件**: 
  - `QualityChecker`: 数据质量检查器
  - `Repository`: ClickHouse数据库操作
  - `BinanceDownloader`: 币安API交互

### 现有代码模式
- Repository提供数据库查询方法：`GetFirstDate`, `GetLastDate`, `GetMonthlyDataStats`
- QualityChecker依赖Downloader获取币安可用日期：`GetAvailableDates`
- 命令行接口通过`verifyData`函数实现

### 业务域理解
- **核心业务**: 币安历史K线数据的下载、存储和质量检查
- **数据完整性**: 每月应有约43,200条1分钟K线记录（30天×24小时×60分钟）
- **数据范围**: 不同交易对有不同的上市时间和数据可用性

## 原始需求分析

### 用户反馈的问题
1. **错误现象**: `go run cmd/main.go -cmd=verify-data -symbols=DFUSDT`显示"未找到可用的月份数据"且质量评分为0%
2. **实际情况**: 数据库中确实存在DFUSDT从2023年开始的数据
3. **期望行为**: 基于数据库实际数据检查月份完整性，报告缺失月份

### 当前实现的问题
```go
// 问题代码：依赖币安API获取可用月份
availableDates, err := qc.downloader.GetAvailableDates(ctx, symbol)
```

**根本问题**: 
- 当前`verify-data`命令依赖`BinanceDownloader.GetAvailableDates()`获取币安API的可用日期
- 但用户需要的是检查**数据库中实际存储的数据**的完整性
- 币安API可用性与数据库实际数据可能不一致

## 边界确认

### 任务范围
✅ **包含**:
- 重新设计`verify-data`命令逻辑
- 基于数据库查询交易对实际数据范围
- 按月检查数据完整性
- 生成详细的月份缺失报告
- 保持现有命令接口不变

❌ **不包含**:
- 修改其他命令（如`check-quality`）
- 改变数据库结构
- 修改币安下载逻辑
- 改变命令行参数格式

### 技术约束
- 必须复用现有Repository方法
- 保持与现有架构一致
- 不能破坏现有功能
- 必须向后兼容

## 需求理解

### 核心需求
用户需要一个**基于数据库的数据完整性检查工具**，具体要求：

1. **数据范围查询**: 从数据库查询某个交易对的实际数据时间范围
   - 示例：DFUSDT从2023年1月到2025年10月
   
2. **月份完整性检查**: 检查数据库中每个月的1分钟K线数据是否完整
   - 每月预期记录数：约43,200条（根据月份天数计算）
   - 实际记录数：从数据库查询
   - 完整性比例：实际/预期
   
3. **缺失报告**: 明确报告哪些月份的数据缺失或不完整
   - 完全缺失的月份
   - 部分缺失的月份（完整性<100%）
   - 数据质量评分

### 功能特性
- **完全基于数据库**: 不依赖币安API
- **灵活符号过滤**: 支持单个或多个交易对
- **详细月份报告**: 逐月显示完整性状态
- **性能优化**: 复用现有查询方法

## 疑问澄清

### 已明确的问题
1. **Q**: 是否需要修改命令行接口？
   **A**: 否，保持现有接口不变
   
2. **Q**: 是否需要支持时间范围过滤？
   **A**: 是，但基于数据库实际数据范围，不是币安API
   
3. **Q**: 如何计算月份完整性？
   **A**: 使用现有的`calculateExpectedRecordsForMonth`方法

### 技术实现决策
1. **数据范围获取**: 使用`Repository.GetFirstDate`和`Repository.GetLastDate`
2. **月份统计**: 使用`Repository.GetMonthlyDataStats`
3. **完整性计算**: 复用现有的`ExpectedRecordsCalculator`
4. **报告格式**: 复用现有的`Reporter`组件

## 初步解决方案

### 方案A: 最小改动方案
- 只修改`getAvailableMonthsForSymbol`方法
- 改为从数据库查询实际数据范围
- 优点：改动最小
- 缺点：逻辑不够清晰

### 方案B: 重构方案（推荐）
- 创建新的`getDatabaseDataRange`方法
- 重写`verifyData`函数的核心逻辑
- 完全基于数据库的数据完整性检查
- 优点：逻辑清晰，功能完整
- 缺点：改动较大

## 验收标准

1. **功能验收**:
   - `go run cmd/main.go -cmd=verify-data -symbols=DFUSDT`能正确显示数据范围
   - 能准确报告每月数据完整性
   - 能识别缺失的月份
   
2. **性能验收**:
   - 查询响应时间<2秒
   - 内存使用合理
   
3. **兼容性验收**:
   - 现有命令行参数正常工作
   - 不影响其他命令功能
   - 输出格式保持一致

## 下一步行动

进入**Architect阶段**，设计基于数据库的数据完整性检查架构。