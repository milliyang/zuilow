# OpenTimestamps 时间戳服务

## 功能概述

本服务为 PaperTrade 账户提供基于区块链的时间戳证明，确保：

1. **数据可追溯**：所有交易操作、收益曲线等信息都有时间戳记录
2. **防篡改**：通过比特币区块链锚定，确保数据无法被篡改
3. **时间证明**：证明数据在下一个交易日之前就已存在

## 工作原理

1. **数据收集**：每天在指定时刻收集所有账户的：
   - 账户信息（现金、初始资金等）
   - 持仓信息
   - 订单历史
   - 成交记录
   - 收益曲线（净值历史）
   - 绩效分析（夏普比率、最大回撤等）

2. **生成原始记录**：将所有数据整理成 JSON 格式的原始记录文件

3. **提交时间戳**：将原始记录提交到 OpenTimestamps 服务，获得区块链时间戳证明

4. **保存证明**：保存原始记录文件和证明文件（.ots），供第三方验证

5. **可选：GitHub 提交**：自动将记录和证明文件提交到 GitHub 仓库

## 安装依赖

```bash
pip install opentimestamps-client PyGithub
```

或者安装完整依赖：

```bash
pip install -r requirements.txt
```

## 配置

### 环境变量

在 `.env` 文件中配置：

```bash
# OpenTimestamps 存储目录（可选，默认: run/opentimestamps）
OTS_STORAGE_DIR=run/opentimestamps

# 定时任务配置：每天执行时间戳的时间（格式: HH:MM）
# 默认: 16:00 (收盘后)
OTS_TIMESTAMP_SCHEDULE=16:0

# GitHub 自动提交（可选）
# 设置为 true 启用自动提交到 GitHub
OTS_AUTO_GITHUB=false

# GitHub 配置（启用自动提交时需要）
GITHUB_TOKEN=your_github_token
GITHUB_REPO=owner/repo_name
```

### 定时任务配置

服务会在每天指定时间自动执行时间戳创建。默认时间为 16:00（收盘后）。

可以通过环境变量 `OTS_TIMESTAMP_SCHEDULE` 修改：
- `16:0` - 每天 16:00 执行（单个时间点）
- `16:0,22:0` - 每天 16:00 和 22:00 执行（多个时间点，用逗号分隔）
- `16:0:us_market,22:0:hk_market` - 带标签的多个时间点（用于区分不同市场）
- `off` - 禁用定时任务
- 格式：`HH:MM` 或 `HH:MM,HH:MM` 或 `HH:MM:标签,HH:MM:标签`

**注意**：每个时间点会创建独立的时间戳文件，文件名包含时间戳（如 `record_2026-01-27_16-00-00.json`），避免覆盖。

## API 接口

### 获取时间戳历史

```http
GET /api/ots/history?limit=100
```

返回所有时间戳历史记录。

### 获取指定日期的详细信息

```http
GET /api/ots/detail/2026-01-27
```

返回指定日期的详细时间戳信息。

### 下载原始记录文件

```http
GET /api/ots/record/2026-01-27
```

下载指定日期的原始记录 JSON 文件。

### 下载证明文件

```http
GET /api/ots/proof/2026-01-27
```

下载指定日期的 OpenTimestamps 证明文件（.ots）。

### 手动创建时间戳（Admin）

```http
POST /api/ots/create
```

手动触发时间戳创建（需要 admin 权限）。

### 验证时间戳（Admin）

```http
POST /api/ots/verify/2026-01-27
```

验证指定日期的时间戳证明（需要 admin 权限）。

### 获取服务信息

```http
GET /api/ots/info
```

获取 OpenTimestamps 服务配置信息。

## 使用示例

### 1. 手动创建时间戳

通过 API 手动创建：

```bash
curl -X POST http://localhost:11182/api/ots/create \
  -H "Cookie: session=your_session_cookie"
```

### 2. 查看历史记录

```bash
curl http://localhost:11182/api/ots/history
```

### 3. 下载证明文件

```bash
# 下载原始记录
curl http://localhost:11182/api/ots/record/2026-01-27 \
  -o record_2026-01-27.json

# 下载证明文件
curl http://localhost:11182/api/ots/proof/2026-01-27 \
  -o record_2026-01-27.ots
```

### 4. 验证时间戳

使用 OpenTimestamps 命令行工具验证：

```bash
# 安装工具
pip install opentimestamps-client

# 验证
ots verify record_2026-01-27.ots record_2026-01-27.json
```

## 文件结构

```
run/opentimestamps/
├── records/          # 原始记录文件
│   ├── record_2026-01-27_16-00-00.json    # 16:00 创建的时间戳
│   ├── record_2026-01-27_22-00-00.json    # 22:00 创建的时间戳
│   ├── record_2026-01-27_us_market.json   # 带标签的时间戳（如果配置了标签）
│   └── record_2026-01-28_16-00-00.json
└── proofs/           # 证明文件
    ├── record_2026-01-27_16-00-00.ots
    ├── record_2026-01-27_22-00-00.ots
    ├── record_2026-01-27_us_market.ots
    └── record_2026-01-28_16-00-00.ots
```

**文件名格式说明**：
- `record_YYYY-MM-DD_HH-MM-SS.json` - 包含时间戳，避免同一天多次创建时覆盖
- `record_YYYY-MM-DD_标签.json` - 如果配置了标签（如 `us_market`），使用标签作为后缀

## GitHub 自动提交

如果启用了 GitHub 自动提交，文件会自动提交到仓库的以下路径：

```
opentimestamps/
├── records/
│   └── record_YYYY-MM-DD.json
└── proofs/
    └── record_YYYY-MM-DD.ots
```

### 设置 GitHub 自动提交

1. 创建 GitHub Personal Access Token：
   - 访问：https://github.com/settings/tokens
   - 创建 token，需要 `repo` 权限

2. 配置环境变量：
   ```bash
   OTS_AUTO_GITHUB=true
   GITHUB_TOKEN=your_token_here
   GITHUB_REPO=your_username/your_repo
   ```

3. 重启服务，时间戳创建后会自动提交到 GitHub

## 第三方验证

任何人都可以独立验证时间戳：

1. **获取文件**：
   - 从服务器下载原始记录和证明文件
   - 或从 GitHub 仓库获取

2. **验证证明**：
   ```bash
   ots verify record_2026-01-27.ots record_2026-01-27.json
   ```

3. **检查时间**：
   验证输出会显示区块时间，确认数据在指定日期之前存在

## 注意事项

1. **时间延迟**：OpenTimestamps 需要等待比特币区块确认，可能需要 10-60 分钟

2. **证明文件**：必须保存 `.ots` 证明文件才能进行验证

3. **网络要求**：需要能够访问 OpenTimestamps 日历服务器

4. **存储空间**：记录文件会随时间增长，建议定期清理旧文件

5. **GitHub 提交**：如果启用自动提交，确保 GitHub token 有足够权限

## 故障排除

### 时间戳提交失败

- 检查 `opentimestamps-client` 是否已安装
- 检查网络连接，确保能访问 OpenTimestamps 服务器
- 查看日志获取详细错误信息

### GitHub 提交失败

- 检查 `GITHUB_TOKEN` 是否正确
- 检查 `GITHUB_REPO` 格式是否正确（owner/repo）
- 确保 token 有 `repo` 权限

### 验证失败

- 确保证明文件已完全下载
- 检查原始记录文件是否未被修改
- 等待区块链确认完成后再验证

## 相关链接

- [OpenTimestamps 官网](https://opentimestamps.org/)
- [OpenTimestamps 文档](https://github.com/opentimestamps/opentimestamps-client)
- [比特币区块链浏览器](https://blockstream.info/)
