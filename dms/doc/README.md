# DMS 文档索引

本文档目录包含 DMS (Data Maintenance Service) 的所有技术文档。

## 文档结构

### 核心文档

- **[架构设计](architecture.md)** - 系统架构概览、核心设计、独立项目设计、目录结构
- **[数据质量](data_quality.md)** - 数据质量保证、自动恢复、可靠性机制
- **[同步策略](sync_strategy.md)** - 同步策略详细设计、性能优化、状态管理
- **[API 参考](api_reference.md)** - HTTP API 完整参考文档
- **[主从模式](master_slave.md)** - 主从架构设计和实现

## 快速导航

### 新手入门

1. 阅读 [架构设计](architecture.md) 了解系统整体设计
2. 阅读 [数据质量](data_quality.md) 了解自动恢复机制
3. 查看配置文件（`config/dms.yaml`, `config/task.yaml`, `config/sync.yaml`）学习如何配置 DMS

### 技术文档

1. [架构设计](architecture.md) - 系统架构和设计原则
2. [数据质量](data_quality.md) - 自动恢复机制、数据质量保证
3. [同步策略](sync_strategy.md) - 同步机制和性能优化
4. [主从模式](master_slave.md) - 主从节点配置和部署
5. [API 参考](api_reference.md) - HTTP API 接口文档

## 文档更新

- **最后更新**: 2026-02
- **当前版本**: 2.0
- **状态**: 核心功能已完成
  - ✅ 增量更新任务
  - ✅ 增量同步
  - ✅ 主从架构
  - ✅ HTTP API
  - ✅ Web UI

## 相关链接

- [项目 README](../README.md) - 项目使用说明
