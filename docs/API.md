# Data4BT API 文档

本文档描述了Data4BT项目提供的HTTP API接口，包括监控、状态查询和管理功能。

## 目录

- [概述](#概述)
- [基础信息](#基础信息)
- [认证](#认证)
- [监控API](#监控api)
- [状态API](#状态api)
- [管理API](#管理api)
- [错误处理](#错误处理)
- [示例代码](#示例代码)

## 概述

Data4BT提供了RESTful API接口，用于监控数据下载进度、查询系统状态和执行管理操作。API服务在数据加载器运行时自动启动。

### 功能特性

- 📊 **实时监控**: 查看下载进度和系统状态
- 🔍 **详细信息**: 获取交易对级别的详细进度
- 🏥 **健康检查**: 系统健康状态监控
- 📈 **性能指标**: 系统资源使用情况
- 🎛️ **管理操作**: 远程控制和配置

## 基础信息

### 服务地址

- **默认地址**: `http://localhost:8890`
- **协议**: HTTP/1.1
- **数据格式**: JSON
- **字符编码**: UTF-8

### 版本信息

- **API版本**: v1
- **兼容性**: 向后兼容

## 认证

当前版本的API不需要认证，但建议仅在受信任的网络环境中使用。

> **安全提示**: 在生产环境中，建议通过防火墙或网络策略限制API访问。

## 监控API

### 获取总体进度

获取所有交易对的下载进度概览。

```http
GET /progress
```

#### 响应示例

```json
{
  "status": "running",
  "total_symbols": 150,
  "completed_symbols": 45,
  "progress_percentage": 30.0,
  "current_symbol": "BTCUSDT",
  "estimated_completion": "2024-01-15T18:30:00Z",
  "start_time": "2024-01-15T10:00:00Z",
  "elapsed_time": "8h30m",
  "download_speed": "2.5 MB/s",
  "total_downloaded": "1.2 GB",
  "errors_count": 3
}
```

#### 字段说明

| 字段 | 类型 | 描述 |
|------|------|------|
| `status` | string | 运行状态: `running`, `completed`, `paused`, `error` |
| `total_symbols` | integer | 总交易对数量 |
| `completed_symbols` | integer | 已完成交易对数量 |
| `progress_percentage` | float | 完成百分比 |
| `current_symbol` | string | 当前处理的交易对 |
| `estimated_completion` | string | 预计完成时间 (ISO 8601) |
| `start_time` | string | 开始时间 (ISO 8601) |
| `elapsed_time` | string | 已用时间 |
| `download_speed` | string | 当前下载速度 |
| `total_downloaded` | string | 已下载总量 |
| `errors_count` | integer | 错误次数 |

### 获取详细进度

获取每个交易对的详细下载进度。

```http
GET /progress/detailed
```

#### 响应示例

```json
{
  "overview": {
    "status": "running",
    "total_symbols": 150,
    "completed_symbols": 45,
    "progress_percentage": 30.0
  },
  "symbols": [
    {
      "symbol": "BTCUSDT",
      "status": "downloading",
      "progress_percentage": 75.0,
      "total_files": 48,
      "completed_files": 36,
      "current_file": "BTCUSDT-1m-2024-01.csv",
      "file_size": "125.6 MB",
      "downloaded_size": "94.2 MB",
      "download_speed": "3.2 MB/s",
      "estimated_completion": "2024-01-15T15:45:00Z",
      "errors": []
    },
    {
      "symbol": "ETHUSDT",
      "status": "completed",
      "progress_percentage": 100.0,
      "total_files": 48,
      "completed_files": 48,
      "completion_time": "2024-01-15T14:30:00Z",
      "total_size": "89.3 MB",
      "errors": []
    }
  ]
}
```

### 获取单个交易对进度

获取指定交易对的详细进度信息。

```http
GET /progress/symbol/{symbol}
```

#### 路径参数

| 参数 | 类型 | 描述 |
|------|------|------|
| `symbol` | string | 交易对名称，如 `BTCUSDT` |

#### 响应示例

```json
{
  "symbol": "BTCUSDT",
  "status": "downloading",
  "progress_percentage": 75.0,
  "total_files": 48,
  "completed_files": 36,
  "current_file": "BTCUSDT-1m-2024-01.csv",
  "file_details": {
    "filename": "BTCUSDT-1m-2024-01.csv",
    "size": "125.6 MB",
    "downloaded": "94.2 MB",
    "progress": 75.0,
    "speed": "3.2 MB/s",
    "eta": "00:10:15"
  },
  "timeline": {
    "start_date": "2017-08-17",
    "end_date": "2024-01-31",
    "total_months": 77,
    "completed_months": 58
  },
  "statistics": {
    "total_size": "6.2 GB",
    "downloaded_size": "4.7 GB",
    "average_speed": "2.8 MB/s",
    "elapsed_time": "2h45m",
    "estimated_completion": "2024-01-15T16:20:00Z"
  },
  "errors": []
}
```

## 状态API

### 健康检查

检查系统健康状态和服务可用性。

```http
GET /health
```

#### 响应示例

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T15:30:00Z",
  "version": "1.2.0",
  "uptime": "8h30m15s",
  "components": {
    "database": {
      "status": "healthy",
      "response_time": "5ms",
      "last_check": "2024-01-15T15:29:55Z"
    },
    "downloader": {
      "status": "healthy",
      "active_connections": 5,
      "queue_size": 12
    },
    "importer": {
      "status": "healthy",
      "buffer_usage": "45%",
      "last_flush": "2024-01-15T15:29:30Z"
    }
  },
  "system": {
    "memory_usage": "512 MB",
    "cpu_usage": "25%",
    "disk_usage": "15.2 GB",
    "goroutines": 45
  }
}
```

### 系统信息

获取系统配置和运行时信息。

```http
GET /info
```

#### 响应示例

```json
{
  "application": {
    "name": "Data4BT",
    "version": "1.2.0",
    "build_time": "2024-01-10T10:00:00Z",
    "git_commit": "a1b2c3d4",
    "go_version": "go1.21.5"
  },
  "configuration": {
    "database": {
      "host": "localhost:9000",
      "database": "binance_data",
      "compression": "lz4"
    },
    "downloader": {
      "concurrency": 5,
      "buffer_size": 1024,
      "compression": true
    },
    "scheduler": {
      "batch_days": 7,
      "concurrent_symbols": 5
    }
  },
  "runtime": {
    "start_time": "2024-01-15T10:00:00Z",
    "uptime": "8h30m15s",
    "pid": 12345,
    "working_directory": "/path/to/data4bt"
  }
}
```

### 性能指标

获取详细的性能指标和统计信息。

```http
GET /metrics
```

#### 响应示例

```json
{
  "timestamp": "2024-01-15T15:30:00Z",
  "performance": {
    "download": {
      "total_files": 1250,
      "completed_files": 945,
      "failed_files": 8,
      "average_speed": "2.8 MB/s",
      "peak_speed": "5.2 MB/s",
      "total_downloaded": "12.5 GB"
    },
    "import": {
      "total_records": 15000000,
      "imported_records": 11250000,
      "import_rate": "5000 records/s",
      "batch_size": 1000,
      "buffer_usage": "45%"
    },
    "database": {
      "connections": {
        "active": 3,
        "idle": 2,
        "max": 10
      },
      "queries": {
        "total": 8500,
        "successful": 8485,
        "failed": 15,
        "average_duration": "12ms"
      }
    }
  },
  "system": {
    "memory": {
      "allocated": "512 MB",
      "heap_size": "256 MB",
      "gc_count": 45
    },
    "cpu": {
      "usage": "25%",
      "goroutines": 45
    },
    "disk": {
      "total_space": "100 GB",
      "used_space": "15.2 GB",
      "available_space": "84.8 GB"
    }
  }
}
```

## 管理API

### 暂停下载

暂停当前的下载任务。

```http
POST /control/pause
```

#### 响应示例

```json
{
  "status": "success",
  "message": "下载任务已暂停",
  "timestamp": "2024-01-15T15:30:00Z"
}
```

### 恢复下载

恢复已暂停的下载任务。

```http
POST /control/resume
```

#### 响应示例

```json
{
  "status": "success",
  "message": "下载任务已恢复",
  "timestamp": "2024-01-15T15:30:00Z"
}
```

### 停止服务

优雅地停止数据加载器服务。

```http
POST /control/shutdown
```

#### 请求体

```json
{
  "force": false,
  "timeout": 30
}
```

#### 参数说明

| 参数 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `force` | boolean | false | 是否强制停止 |
| `timeout` | integer | 30 | 优雅关闭超时时间（秒） |

#### 响应示例

```json
{
  "status": "success",
  "message": "服务正在优雅关闭",
  "timestamp": "2024-01-15T15:30:00Z",
  "estimated_shutdown_time": "2024-01-15T15:30:30Z"
}
```

## 错误处理

### 错误响应格式

所有API错误都使用统一的响应格式：

```json
{
  "error": {
    "code": "SYMBOL_NOT_FOUND",
    "message": "指定的交易对不存在",
    "details": {
      "symbol": "INVALIDUSDT",
      "available_symbols": ["BTCUSDT", "ETHUSDT"]
    },
    "timestamp": "2024-01-15T15:30:00Z"
  }
}
```

### HTTP状态码

| 状态码 | 描述 | 示例场景 |
|--------|------|----------|
| 200 | 成功 | 正常请求 |
| 400 | 请求错误 | 参数格式错误 |
| 404 | 资源不存在 | 交易对不存在 |
| 500 | 服务器错误 | 内部错误 |
| 503 | 服务不可用 | 系统维护中 |

### 常见错误代码

| 错误代码 | 描述 | 解决方案 |
|----------|------|----------|
| `SYMBOL_NOT_FOUND` | 交易对不存在 | 检查交易对名称 |
| `SERVICE_UNAVAILABLE` | 服务不可用 | 等待服务恢复 |
| `INVALID_PARAMETER` | 参数无效 | 检查请求参数 |
| `RATE_LIMIT_EXCEEDED` | 请求频率过高 | 降低请求频率 |
| `INTERNAL_ERROR` | 内部错误 | 联系技术支持 |

## 示例代码

### JavaScript/Node.js

```javascript
// 获取总体进度
async function getProgress() {
  try {
    const response = await fetch('http://localhost:8890/progress');
    const data = await response.json();
    console.log('下载进度:', data.progress_percentage + '%');
    return data;
  } catch (error) {
    console.error('获取进度失败:', error);
  }
}

// 获取特定交易对进度
async function getSymbolProgress(symbol) {
  try {
    const response = await fetch(`http://localhost:8890/progress/symbol/${symbol}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error(`获取${symbol}进度失败:`, error);
  }
}

// 健康检查
async function healthCheck() {
  try {
    const response = await fetch('http://localhost:8890/health');
    const data = await response.json();
    return data.status === 'healthy';
  } catch (error) {
    console.error('健康检查失败:', error);
    return false;
  }
}

// 使用示例
(async () => {
  // 检查服务健康状态
  const isHealthy = await healthCheck();
  if (!isHealthy) {
    console.log('服务不健康，请检查');
    return;
  }
  
  // 获取总体进度
  const progress = await getProgress();
  console.log(`当前进度: ${progress.progress_percentage}%`);
  
  // 获取BTCUSDT进度
  const btcProgress = await getSymbolProgress('BTCUSDT');
  if (btcProgress) {
    console.log(`BTCUSDT进度: ${btcProgress.progress_percentage}%`);
  }
})();
```

### Python

```python
import requests
import json
from typing import Optional, Dict, Any

class Data4BTAPI:
    def __init__(self, base_url: str = "http://localhost:8890"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def get_progress(self) -> Optional[Dict[str, Any]]:
        """获取总体进度"""
        try:
            response = self.session.get(f"{self.base_url}/progress")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"获取进度失败: {e}")
            return None
    
    def get_symbol_progress(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取特定交易对进度"""
        try:
            response = self.session.get(f"{self.base_url}/progress/symbol/{symbol}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"获取{symbol}进度失败: {e}")
            return None
    
    def health_check(self) -> bool:
        """健康检查"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            response.raise_for_status()
            data = response.json()
            return data.get('status') == 'healthy'
        except requests.RequestException as e:
            print(f"健康检查失败: {e}")
            return False
    
    def pause_download(self) -> bool:
        """暂停下载"""
        try:
            response = self.session.post(f"{self.base_url}/control/pause")
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            print(f"暂停下载失败: {e}")
            return False
    
    def resume_download(self) -> bool:
        """恢复下载"""
        try:
            response = self.session.post(f"{self.base_url}/control/resume")
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            print(f"恢复下载失败: {e}")
            return False

# 使用示例
if __name__ == "__main__":
    api = Data4BTAPI()
    
    # 健康检查
    if not api.health_check():
        print("服务不健康，请检查")
        exit(1)
    
    # 获取总体进度
    progress = api.get_progress()
    if progress:
        print(f"当前进度: {progress['progress_percentage']}%")
        print(f"当前处理: {progress['current_symbol']}")
    
    # 获取特定交易对进度
    btc_progress = api.get_symbol_progress('BTCUSDT')
    if btc_progress:
        print(f"BTCUSDT进度: {btc_progress['progress_percentage']}%")
```

### Bash/Shell

```bash
#!/bin/bash
# Data4BT API 客户端脚本

API_BASE="http://localhost:8890"

# 获取总体进度
get_progress() {
    curl -s "$API_BASE/progress" | jq -r '.progress_percentage'
}

# 获取特定交易对进度
get_symbol_progress() {
    local symbol="$1"
    curl -s "$API_BASE/progress/symbol/$symbol" | jq -r '.progress_percentage'
}

# 健康检查
health_check() {
    local status
    status=$(curl -s "$API_BASE/health" | jq -r '.status')
    [ "$status" = "healthy" ]
}

# 暂停下载
pause_download() {
    curl -s -X POST "$API_BASE/control/pause" | jq -r '.message'
}

# 恢复下载
resume_download() {
    curl -s -X POST "$API_BASE/control/resume" | jq -r '.message'
}

# 监控脚本
monitor() {
    while true; do
        if health_check; then
            local progress
            progress=$(get_progress)
            echo "$(date): 进度 ${progress}%"
        else
            echo "$(date): 服务不健康"
        fi
        sleep 30
    done
}

# 使用示例
case "${1:-}" in
    "progress")
        get_progress
        ;;
    "symbol")
        get_symbol_progress "$2"
        ;;
    "health")
        health_check && echo "健康" || echo "不健康"
        ;;
    "pause")
        pause_download
        ;;
    "resume")
        resume_download
        ;;
    "monitor")
        monitor
        ;;
    *)
        echo "用法: $0 {progress|symbol SYMBOL|health|pause|resume|monitor}"
        exit 1
        ;;
esac
```

## 最佳实践

### 1. 错误处理

- 始终检查HTTP状态码
- 实现重试机制处理临时错误
- 记录详细的错误信息用于调试

### 2. 性能优化

- 使用连接池减少连接开销
- 合理设置请求超时时间
- 避免频繁轮询，使用适当的间隔

### 3. 监控建议

- 定期检查健康状态
- 监控关键性能指标
- 设置告警阈值

### 4. 安全考虑

- 限制API访问来源
- 使用HTTPS（生产环境）
- 实施访问频率限制

## 更新日志

### v1.2.0

- 新增信号处理状态API
- 增强错误响应格式
- 添加性能指标接口
- 改进健康检查功能

### v1.1.0

- 新增管理控制API
- 添加详细进度查询
- 优化响应格式

### v1.0.0

- 初始API版本
- 基础监控功能
- 健康检查接口