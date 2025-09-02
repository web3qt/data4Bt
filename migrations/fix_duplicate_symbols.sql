-- 修复symbol_infos表重复数据的迁移脚本
-- 执行时间: 2025-01-23

-- 1. 备份现有数据
CREATE TABLE symbol_infos_backup AS SELECT * FROM symbol_infos;

-- 2. 删除现有表
DROP TABLE IF EXISTS symbol_infos;

-- 3. 重新创建表，使用ReplacingMergeTree引擎防止重复
CREATE TABLE symbol_infos (
    symbol String,
    status String,
    base_asset String,
    quote_asset String,
    earliest_date Date,
    latest_date Date,
    total_months Int32,
    data_status String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY symbol
SETTINGS index_granularity = 8192;

-- 4. 插入去重后的数据（保留每个symbol的最新记录）
INSERT INTO symbol_infos
SELECT 
    symbol,
    status,
    base_asset,
    quote_asset,
    earliest_date,
    latest_date,
    total_months,
    data_status,
    created_at,
    updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY updated_at DESC, created_at DESC) as rn
    FROM symbol_infos_backup
) ranked
WHERE rn = 1;

-- 5. 验证数据
SELECT 
    'Before migration' as stage,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM symbol_infos_backup

UNION ALL

SELECT 
    'After migration' as stage,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM symbol_infos;

-- 6. 清理备份表（可选，建议保留一段时间）
-- DROP TABLE symbol_infos_backup;