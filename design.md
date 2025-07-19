
### 一、 核心设计原则 (Architectural Principles)

1. **单一职责原则 (SRP)**
2. **依赖倒置原则 (DIP)**: 面向接口编程，而不是面向实现。这使得我们可以轻松替换组件（例如，未来从Binance切换到Bybit，或从ClickHouse切换到其他数据库）。
3. **并发与解耦**: 使用Go的并发原语（Goroutines和Channels）来构建一个**流水线(Pipeline)**式的并发模型，将不同处理阶段解耦，最大化吞吐量。
4. **状态外化**: 将应用的状态（如上次处理到哪里）存储在外部（如文件或数据库），使应用本身无状态，易于重启和横向扩展。
5. **可观测性**: 系统必须提供清晰的日志、指标和进度反馈，以便于监控和调试。

---

### 二、 系统架构图 (Component Diagram)

这是一个典型的生产者-消费者流水线模型。

```
+-----------+       +-------------------+       +-------------------+       +---------------+
|           |       |                   |       |                   |       |               |
|  Scheduler|-----> |      Fetcher      |-----> |      Parser       |-----> |    Loader     |
| (主控&调度)|       |  (并发下载&解压)   |       |  (并发CSV解析)    |       | (批量导入DB)   |
|           |       |                   |       |                   |       |               |
+-----+-----+       +--------+----------+       +--------+----------+       +-------+-------+
      |                      ^                      ^                              |
 (Reads/Writes)              | (Tasks Channel)      | (Parsed Data Channel)        v
      |                      +----------------------+------------------------------+
      |
+-----+-----+       +-------------------+       +-------------------+
|           |       |                   |       |                   |
|   State   |       |   Configuration   |       |      Logger       |  <-- Cross-cutting Concerns
|  Manager  |       |     (Viper)       |       |     (Zerolog)     |      (横切关注点)
|  (JSON)   |       |                   |       |                   |
+-----------+       +-------------------+       +-------------------+
```

### 三、 Go项目结构 (Project Layout)

遵循标准的Go项目布局，便于团队协作和工具集成。

```
/binance-data-loader
├── cmd/
│   └── loader/
│       └── main.go         # 程序入口
├── internal/
│   ├── config/             # 配置加载 (Viper)
│   ├── logger/             # 日志初始化 (Zerolog)
│   ├── state/              # 状态管理
│   ├── pipeline/           # 流水线核心逻辑
│   │   ├── scheduler.go    # 1. 任务调度器
│   │   ├── fetcher.go      # 2. 下载器
│   │   ├── parser.go       # 3. 解析器
│   │   └── loader.go       # 4. 加载器
│   └── domain/             # 领域模型与接口
│       ├── kline.go        # KLine结构体定义
│       ├── repository.go   # 数据库存储接口
│       └── downloader.go   # 下载器接口
└── pkg/
    ├── clickhouse/         # ClickHouse存储接口的具体实现
    └── binance/            # Binance下载器接口的具体实现
├── go.mod
├── go.sum
└── configs/
    └── config.default.yml  # 默认配置文件
```

---

### 四、 核心组件详解 (Go实现思路)

#### 1. `domain/` - 领域模型与接口

这是系统的“契约”，定义了数据是什么以及组件应该做什么。

#### 2. `pkg/` - 接口的具体实现

这里是与外部服务（Binance, ClickHouse）打交道的具体代码。

```go
// pkg/binance/downloader.go
package binance

// BinanceDownloader 实现了 domain.Downloader 接口
type BinanceDownloader struct {
    // ... http client, base_url
}

func (d *BinanceDownloader) Fetch(ctx context.Context, task domain.DownloadTask) ([]byte, error) {
    // 构造URL, 发送HTTP请求, 下载zip文件内容
    // 在内存中解压zip, 返回csv文件的[]byte
}

// pkg/clickhouse/repository.go
package clickhouse

// CHRepository 实现了 domain.KLineRepository 接口
type CHRepository struct {
    // ... clickhouse-go client
}

func (r *CHRepository) Save(ctx context.Context, klines []domain.KLine) error {
    // 构造批量INSERT语句并执行
}
```

#### 3. `internal/pipeline/` - 流水线核心

这里用channels将各个阶段连接起来。

```go
// internal/pipeline/scheduler.go
package pipeline

// Scheduler 负责生成任务
func NewScheduler(stateManager state.Manager, config *config.Config) *Scheduler { /* ... */ }

func (s *Scheduler) GenerateTasks(ctx context.Context, tasksChan chan<- domain.DownloadTask) {
    // 1. 调用Binance API获取所有交易对
    // 2. 对比stateManager中的状态，为每个交易对生成需要下载的日期任务
    // 3. 将任务发送到tasksChan
    // 4. 所有任务生成完毕后，close(tasksChan)
}

// internal/pipeline/fetcher.go
package pipeline

// Fetcher 负责下载和解压
func NewFetcher(downloader domain.Downloader, concurrency int) *Fetcher { /* ... */ }

func (f *Fetcher) Run(ctx context.Context, tasksChan <-chan domain.DownloadTask, rawDataChan chan<- []byte) {
    var wg sync.WaitGroup
    for i := 0; i < f.concurrency; i++ {
        wg.Add(1)
        go func() { // 启动一个工作goroutine
            defer wg.Done()
            for task := range tasksChan { // 从任务通道取任务
                rawData, err := f.downloader.Fetch(ctx, task)
                // ... 错误处理 ...
                rawDataChan <- rawData // 将结果发到下一个通道
            }
        }()
    }
    wg.Wait()
    close(rawDataChan) // 所有下载任务完成后，关闭下一个通道
}

// internal/pipeline/parser.go and loader.go 结构类似，
// parser从rawDataChan取数据，解析后发到parsedDataChan
// loader从parsedDataChan取数据，累积到批次大小后调用repository.Save()
```

#### 4. `cmd/loader/main.go` - 程序入口与组装

`main.go` 的职责就是**组装 (Assemble)** 整个系统。

```go
package main

func main() {
    // 1. 初始化横切关注点
    cfg := config.Load()
    logger.Init(cfg.Log)
    stateMgr := state.NewManager("state.json")
    
    // 2. 依赖注入：创建接口的具体实现实例
    downloader := binance.NewDownloader(cfg.Binance)
    repository := clickhouse.NewRepository(cfg.Database)

    // 3. 组装流水线
    scheduler := pipeline.NewScheduler(stateMgr, cfg)
    fetcher := pipeline.NewFetcher(downloader, cfg.Downloader.Concurrency)
    parser := pipeline.NewParser(...)
    loader := pipeline.NewLoader(repository, cfg.Importer.BatchSize)

    // 4. 创建Channels
    tasksChan := make(chan domain.DownloadTask, 100)
    rawDataChan := make(chan []byte, 100)
    parsedDataChan := make(chan []domain.KLine, cfg.Importer.BatchSize)

    // 5. 启动流水线 (使用errgroup来优雅地管理goroutines和错误)
    g, ctx := errgroup.WithContext(context.Background())

    g.Go(func() error { scheduler.GenerateTasks(ctx, tasksChan); return nil })
    g.Go(func() error { fetcher.Run(ctx, tasksChan, rawDataChan); return nil })
    g.Go(func() error { parser.Run(ctx, rawDataChan, parsedDataChan); return nil })
    g.Go(func() error { loader.Run(ctx, parsedDataChan); return nil })
    
    // 6. 等待所有部分完成或出错
    if err := g.Wait(); err != nil {
        log.Fatal().Err(err).Msg("Pipeline execution failed")
    }
    
    log.Info().Msg("Data loading finished successfully")
}
```

### 五、 部署与运维

- **编译**: `go build -o bin/loader ./cmd/loader`
- **Docker化**: 创建一个`Dockerfile`，将编译好的二进制文件和配置文件打包成一个轻量级镜像。
- **调度**: 使用系统的 `cron` 作业或Kubernetes CronJob来每日定时运行这个Docker容器。
