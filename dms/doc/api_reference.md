# DMS API 参考文档

> 完整的 HTTP API 接口文档

## 基础信息

- **Base URL**: `http://localhost:11183`
- **API Prefix**: `/api/dms`
- **Content-Type**: `application/json`

## 通用 API（主从节点都支持）

### GET /api/dms/status

获取当前节点状态

**响应示例**：
```json
{
  "role": "master",
  "running": true,
  "uptime": 3600,
  "tasks_count": 3
}
```

### GET /api/dms/nodes

获取所有节点状态（主节点返回主+所有从节点，从节点返回主+自己）

**响应示例**：
```json
{
  "nodes": [
    {
      "name": "master",
      "role": "master",
      "host": "master.example.com",
      "port": 11183,
      "status": "running",
      "uptime": 3600,
      "tasks_count": 3,
      "last_sync": null
    },
    {
      "name": "backup1",
      "role": "slave",
      "host": "backup1.example.com",
      "port": 11183,
      "status": "running",
      "uptime": 3600,
      "last_sync": "2024-01-23T10:29:55Z",
      "sync_delay": 5
    }
  ],
  "total_nodes": 2,
  "online_nodes": 2
}
```

### GET /api/dms/sync/status

获取同步状态

**响应示例**：
```json
{
  "backups": [
    {
      "name": "backup1",
      "host": "backup1.example.com",
      "port": 8086,
      "last_sync": "2024-01-23T10:30:00Z"
    }
  ],
  "total_backups": 1
}
```

### GET /api/dms/sync/history

获取同步历史

**查询参数**：
- `backup_name` (可选): 备份节点名称
- `limit` (可选, 默认 100): 返回记录数
- `offset` (可选, 默认 0): 偏移量

**响应示例**：
```json
{
  "history": [
    {
      "id": 1,
      "backup_name": "backup1",
      "symbol": "US.AAPL",
      "interval": "1d",
      "sync_mode": "incremental",
      "start_time": "2024-01-23T10:00:00Z",
      "end_time": "2024-01-23T10:30:00Z",
      "data_count": 1000,
      "status": "success"
    }
  ]
}
```

### POST /api/dms/read/batch

批量读取数据（回测场景）

**请求体**：
```json
{
  "symbols": ["US.AAPL", "US.TSLA", "US.GOOGL"],
  "start_date": "2024-01-01T00:00:00",
  "end_date": "2024-01-23T00:00:00",
  "interval": "1d"
}
```

**响应示例**：
```json
{
  "US.AAPL": {
    "data": [
      {
        "time": "2024-01-01T00:00:00",
        "Open": 150.0,
        "High": 155.0,
        "Low": 149.0,
        "Close": 154.0,
        "Volume": 1000000
      }
    ],
    "index": ["2024-01-01T00:00:00"]
  }
}
```

### GET /api/dms/read/{symbol}

读取单个 symbol 数据

**查询参数**：
- `start_date`: 开始日期 (ISO 格式)
- `end_date`: 结束日期 (ISO 格式)
- `interval`: 时间间隔 (默认 "1d")

**响应示例**：
```json
{
  "symbol": "US.AAPL",
  "data": [
    {
      "time": "2024-01-01T00:00:00",
      "Open": 150.0,
      "High": 155.0,
      "Low": 149.0,
      "Close": 154.0,
      "Volume": 1000000
    }
  ],
  "index": ["2024-01-01T00:00:00"]
}
```

## 主节点专用 API

### POST /api/dms/tasks/trigger

手动触发单个任务

**查询参数**：
- `task_name`: 任务名称

**响应示例**：
```json
{
  "success": true,
  "result": {
    "success": true,
    "message": "Task daily_update_us triggered successfully, running in background",
    "status": "running",
    "task_name": "daily_update_us"
  }
}
```

### POST /api/dms/tasks/trigger-all

触发所有任务（或指定类型的任务）

**查询参数**：
- `task_type` (可选): 任务类型过滤（如 "incremental", "full_sync"），不指定则触发所有任务

**响应示例**：
```json
{
  "success": true,
  "result": {
    "success": true,
    "message": "Triggered 2 task(s), 2 succeeded",
    "triggered_count": 2,
    "success_count": 2,
    "results": {
      "daily_update_us": {
        "success": true,
        "status": "running"
      },
      "daily_update_hk": {
        "success": true,
        "status": "running"
      }
    }
  }
}
```

### GET /api/dms/tasks

获取任务列表

**响应示例**：
```json
[
  {
    "name": "daily_update",
    "status": "idle",
    "last_run_time": "2024-01-23T10:30:00Z"
  }
]
```

### GET /api/dms/tasks/{task_name}/status

获取任务状态

**响应示例**：
```json
{
  "name": "daily_update",
  "status": "completed",
  "last_run_time": "2024-01-23T10:30:00Z",
  "last_result": {
    "success": true,
    "message": "Updated 5 symbols",
    "data_count": 100
  },
  "stats": {
    "total_runs": 100,
    "success_count": 95,
    "failed_count": 5,
    "total_data_count": 10000,
    "avg_duration": 5.2
  }
}
```

### POST /api/dms/sync/trigger

手动触发同步到备份节点

**查询参数**：
- `backup_name` (可选): 备份节点名称（不指定则同步到所有备份节点）

**响应示例**：
```json
{
  "success": true,
  "message": "Sync triggered",
  "result": {
    "success": true,
    "results": {
      "backup1": true,
      "backup2": true
    }
  }
}
```

### GET /api/dms/maintenance/log

获取维护日志

**查询参数**：
- `task_name` (可选): 任务名称
- `limit` (可选, 默认 100): 返回记录数
- `offset` (可选, 默认 0): 偏移量

**响应示例**：
```json
{
  "logs": [
    {
      "id": 1,
      "task_name": "daily_update",
      "task_type": "incremental",
      "start_time": "2024-01-23T10:00:00Z",
      "end_time": "2024-01-23T10:30:00Z",
      "duration": 1800.5,
      "status": "completed",
      "result_message": "Updated 5 symbols",
      "data_count": 100
    }
  ]
}
```

### GET /api/dms/slaves

获取从节点列表

**响应示例**：
```json
[
  {
    "name": "backup1",
    "host": "backup1.example.com",
    "port": 11183,
    "enabled": true
  }
]
```

### GET /api/dms/slaves/{slave_name}/status

获取指定从节点状态（通过 HTTP 查询从节点）

**响应示例**：
```json
{
  "name": "backup1",
  "role": "slave",
  "host": "backup1.example.com",
  "port": 11183,
  "status": "running",
  "uptime": 3600
}
```

### POST /api/dms/slaves/{slave_name}/sync

同步数据到指定从节点

**请求体**：
```json
{
  "symbol": "US.AAPL",
  "start_date": "2024-01-01T00:00:00",
  "end_date": "2024-01-23T00:00:00"
}
```

**响应示例**：
```json
{
  "success": true,
  "result": {
    "success": true,
    "message": "Sync request queued for backup1"
  }
}
```

## 从节点专用 API

### POST /api/dms/sync/request

请求主节点同步数据到本地

**请求体**：
```json
{
  "symbol": "US.AAPL",
  "start_date": "2024-01-01T00:00:00",
  "end_date": "2024-01-23T00:00:00"
}
```

**响应示例**：
```json
{
  "success": true,
  "result": {
    "success": true,
    "message": "Sync completed"
  }
}
```

### GET /api/dms/master/status

获取主节点状态（通过 HTTP 查询主节点）

**响应示例**：
```json
{
  "role": "master",
  "running": true,
  "uptime": 3600,
  "tasks_count": 3
}
```

## 节点状态查询机制

### 主节点查询从节点

主节点通过 HTTP API 主动查询所有从节点的状态：
- `GET http://{slave_host}:{slave_port}/api/dms/status`
- 聚合所有节点状态，返回统一格式

### 从节点查询主节点

从节点通过 HTTP API 查询主节点状态：
- `GET http://{master_host}:{master_port}/api/dms/status`
- 同时返回自己的状态

## 错误响应

所有 API 在出错时返回标准错误格式：

```json
{
  "detail": "Error message"
}
```

**HTTP 状态码**：
- `200` - 成功
- `400` - 请求参数错误
- `403` - 权限不足（例如从节点访问主节点专用 API）
- `404` - 资源不存在
- `500` - 服务器内部错误
- `503` - 服务未初始化

## 相关文档

- [架构设计](architecture.md) - 系统架构概览
- [主从模式](master_slave.md) - 主从通信机制
