# 同步策略

> DMS 使用增量同步策略，在数据维护任务完成后自动同步到备份节点。

## 一、同步方式

**增量同步（Incremental Sync）**

DMS 采用增量同步方式，只同步新增或更新的数据，避免重复传输。

**工作原理**：
1. 记录每个备份节点的最后同步时间
2. 只同步上次同步时间之后的新数据
3. 首次同步时，同步配置的历史数据范围（默认5年）
4. 同步失败时自动重试，支持断点续传

## 二、配置说明

### 2.1 配置文件

**`config/sync.yaml`** - 同步配置

```yaml
sync:
  default_mode: incremental    # 同步模式
  
  incremental:
    enabled: true
    check_interval: 60        # 检查间隔（秒）
    batch_size: 5000          # 批量大小
    max_gap_hours: 24         # 最大时间间隔（小时）
    initial_days: 1825        # 首次同步历史范围（天，5年）
  
  performance:
    max_workers: 5            # 并行同步线程数
    connection_pool_size: 5   # 连接池大小
  
  retry_times: 3              # 失败重试次数
  retry_delay: 5              # 重试延迟（秒）
  retry_backoff: exponential  # 重试策略：exponential, linear
```

### 2.2 任务配置

**`config/task.yaml`** - 任务级同步配置

```yaml
tasks:
  - name: daily_update_us
    type: incremental
    sync_backups: true        # 启用同步到备份节点
    initial_days: 1825        # 首次获取历史数据范围（天）
```

### 2.3 配置参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `initial_days` | 首次同步/获取的历史数据天数 | 1825（5年） |
| `batch_size` | 批量同步的记录数 | 5000 |
| `max_workers` | 并行同步的线程数 | 5 |
| `retry_times` | 失败重试次数 | 3 |
| `retry_delay` | 重试延迟（秒） | 5 |

**建议值**：
- **回测需求**：`initial_days: 1825` (5年)
- **长期回测**：`initial_days: 3650` (10年)
- **仅最近数据**：`initial_days: 365` (1年)

## 三、工作流程

### 3.1 维护任务同步流程

```
维护任务执行 → 获取新数据 → 写入主库 → 触发增量同步 → 并行同步到备份节点
                                                ↓
                                        记录同步历史/更新同步时间
```

### 3.2 首次同步

当备份节点首次连接时：
1. 检查同步历史，未找到记录
2. 使用 `initial_days` 配置确定开始时间
3. 从主库读取历史数据
4. 写入备份节点
5. 记录同步时间

### 3.3 增量同步

后续同步时：
1. 查询该备份节点的最后同步时间
2. 读取上次同步时间之后的新数据
3. 写入备份节点
4. 更新同步时间

### 3.4 断点续传

同步失败时：
- 记录失败状态和错误信息
- 不更新同步时间
- 下次同步时从上次成功的时间点继续
- 支持自动重试（指数退避）

## 四、性能优化

### 4.1 并行同步

```python
# 多个备份节点并行同步
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for backup in enabled_backups:
        future = executor.submit(
            self.sync_incremental, 
            backup_name, 
            symbol, 
            interval
        )
        futures.append((backup_name, future))
```

### 4.2 批量读写

- 批量读取：一次读取多条记录，减少数据库查询次数
- 批量写入：使用 InfluxDB 的批量写入 API
- 默认批量大小：5000 条记录

### 4.3 连接池

- 复用数据库连接，避免频繁建立连接
- 连接池大小：默认 5 个连接

### 4.4 重试机制

**指数退避（Exponential Backoff）**：

```
重试次数    延迟时间
  1         5秒
  2         10秒
  3         20秒
```

避免在网络故障时过度重试，给系统恢复时间。

## 五、同步历史

### 5.1 存储位置

同步历史存储在 SQLite 数据库中：
- 路径：`dms/run/db/sync_history.db`
- 表：`sync_history` - 同步记录
- 表：`sync_status` - 最后同步时间

### 5.2 记录内容

每次同步记录包含：
- 备份节点名称
- 股票代码
- 时间间隔
- 同步模式
- 开始/结束时间
- 同步状态（running, success, failed）
- 数据条数
- 错误信息（如果失败）

### 5.3 查询同步历史

通过 HTTP API 查询：

```bash
# 查询所有同步历史
GET /api/dms/sync/history

# 查询特定备份节点的同步历史
GET /api/dms/sync/history?backup=backup1

# 查询特定股票的同步历史
GET /api/dms/sync/history?symbol=US.AAPL
```

## 六、监控和维护

### 6.1 监控指标

| 指标 | 说明 |
|------|------|
| 同步成功率 | 成功同步次数 / 总同步次数 |
| 平均同步时间 | 单次同步平均耗时 |
| 同步延迟 | 主库最新时间 - 备份节点最新时间 |
| 失败次数 | 同步失败的次数 |

### 6.2 常见问题

**问题 1：同步延迟过大**

原因：
- 网络带宽不足
- 备份节点性能不足
- 批量大小配置不合理

解决：
- 增加 `batch_size`
- 增加 `max_workers`
- 检查网络连接

**问题 2：同步频繁失败**

原因：
- 备份节点不可达
- 认证失败
- 数据库空间不足

解决：
- 检查备份节点连接
- 验证认证信息
- 检查磁盘空间

**问题 3：首次同步时间过长**

原因：
- `initial_days` 配置过大
- 网络速度慢

解决：
- 减小 `initial_days`
- 分批次同步（手动执行多次增量同步）

## 七、最佳实践

1. **合理设置 initial_days**
   - 根据实际回测需求设置
   - 不要设置过大，避免首次同步时间过长

2. **监控同步状态**
   - 定期检查同步历史
   - 设置告警通知

3. **定期清理历史记录**
   - 同步历史会不断增长
   - 可以定期清理旧记录

4. **备份节点规划**
   - 不要配置过多备份节点
   - 建议 2-3 个备份节点

5. **网络规划**
   - 备份节点尽量在同一网络
   - 考虑使用专用网络

## 相关文档

- [架构设计](architecture.md) - 系统架构和设计
- [主从模式](master_slave.md) - 主从架构详细设计
- [API 参考](api_reference.md) - 同步相关 API 接口
