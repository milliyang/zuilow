# 主从模式设计

> DMS 主从架构详细设计和实现说明

## 一、概述

DMS 支持主从架构，从节点可以作为**完全备份角色**，提供数据冗余和负载分担。

## 二、角色定义

### 主节点（Master）角色

- **主动维护**：执行所有维护任务（增量更新、全量同步、数据校验等）
- **数据获取**：从 YFinance/Futu 等外部数据源获取数据
- **数据写入**：写入主数据库
- **同步推送**：主动同步数据到从节点
- **任务调度**：启动和维护调度器，执行定时任务

### 从节点（Slave/Backup）角色

- **被动同步**：只接受来自主节点的数据库更新
- **不主动获取**：不会主动获取 YFinance 等信息，不执行维护任务
- **手动触发同步**：可以手动触发，通知主节点同步数据到本地
- **完整读取功能**：提供与主节点一样的数据读取功能（Reader）
- **统一 Web 界面**：提供与主节点相同的 Web 管理页面

## 三、主从通信

### 通信方式

- **主 → 从**：主节点主动推送数据更新到从节点
- **从 → 主**：从节点可以请求主节点同步数据（HTTP API）
- **状态同步**：主从节点可以互相查询状态

### HTTP API 通信

主从节点通过 HTTP API 进行通信：

- 主节点查询从节点：`GET http://{slave_host}:{slave_port}/api/dms/status`
- 从节点查询主节点：`GET http://{master_host}:{master_port}/api/dms/status`
- 从节点请求同步：`POST http://{master_host}:{master_port}/api/dms/slaves/{slave_name}/sync`
- 主节点同步到从节点：`POST http://{slave_host}:{slave_port}/api/dms/sync/receive`（内部使用）

## 五、实现细节


### 5.2 主节点主动推送

```python
# 主节点写入数据后，主动推送到从节点
class MasterNode:
    def write_data(self, symbol, data, interval):
        # 写入主数据库
        self.writer.write_data(symbol, data, interval)
        
        # 推送到所有从节点
        for slave in self.slaves:
            if slave.enabled:
                self.sync_manager.push_to_slave(slave, symbol, data, interval)
```

### 5.3 从节点请求同步

```python
# 从节点请求主节点同步数据
class SlaveNode:
    def request_sync(self, symbol=None, start_date=None, end_date=None):
        """请求主节点同步数据到本地"""
        response = requests.post(
            f"http://{self.master.host}:{self.master.port}/api/dms/slaves/{self.name}/sync",
            json={
                "symbol": symbol,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
            }
        )
        return response.json()
```

## 六、Web 界面

### 统一状态展示

主节点和从节点都提供相同的 Web 界面，但展示的节点范围不同。

#### 主节点界面

- **节点状态面板**：显示主节点 + 所有从节点的状态
- **主节点功能**：维护任务列表、同步任务管理、维护日志查看
- **从节点状态**：从节点列表、同步状态、同步历史记录

#### 从节点界面

- **节点状态面板**：显示主节点 + 自己的状态
- **从节点功能**：数据读取功能、同步状态查看、手动请求同步

详细界面设计请参考：[架构设计 - Web 界面](architecture.md#九web-界面设计)

## 七、状态查询机制

### 主节点查询从节点

主节点通过 HTTP API 主动查询所有从节点的状态，聚合后返回统一格式。

### 从节点查询主节点

从节点通过 HTTP API 查询主节点状态，同时返回自己的状态。

### 统一状态格式

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

## 八、优势

1. **数据冗余**：从节点提供完整数据备份
2. **负载分担**：从节点可以分担读取负载
3. **高可用**：主节点故障时可以从从节点恢复
4. **灵活部署**：主从节点可以部署在不同位置
5. **统一管理**：统一的 Web 界面管理所有节点

## 相关文档

- [架构设计](architecture.md) - 系统整体架构
- [API 参考](api_reference.md) - 主从通信 API
- 配置文件: `config/dms.example.yaml` - 查看主从配置示例
