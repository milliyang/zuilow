# DMS Docker 部署

Docker 相关配置文件都放在此目录下。

## 文件说明

- `Dockerfile` - Docker 镜像构建文件
- `docker-compose.yml` - Docker Compose 配置文件（包含 InfluxDB）
- `docker-compose.external-db.yml` - 使用外部 InfluxDB 的配置
- `env.example` - 环境变量模板（复制为 `.env` 使用）
- `.dockerignore` - Docker 构建忽略文件（参考文件，实际使用项目根目录的 `.dockerignore`）
- `docker_tools.sh` - Docker 管理脚本
- `README.md` - 本说明文件

**注意**：
- `.env` 文件会在运行 `init` 命令时自动创建在 `docker/` 目录下
- `.dockerignore` 在项目根目录也有一份（Docker 构建时需要）

## 使用方法

**注意：所有命令都需要在项目根目录（`sai/dms/`）下执行**

```bash
# 1. 初始化环境（创建 .env 和必要目录）
./docker/docker_tools.sh init

# 2. 编辑配置文件
# - 编辑 docker/.env 文件（从 docker/env.example 复制）
# - 编辑 config/dms.yaml, config/task.yaml, config/sync.yaml

# 3. 构建并启动
./docker/docker_tools.sh build
./docker/docker_tools.sh up

# 查看日志
./docker/docker_tools.sh logs        # 所有服务
./docker/docker_tools.sh logs dms    # 仅 DMS 服务

# 停止服务
./docker/docker_tools.sh down
```

## 访问地址

- **DMS WebUI**: http://localhost:11183
- **InfluxDB UI**: http://localhost:8086（仅在使用 Docker 内 InfluxDB 时）

## InfluxDB 配置方式 ⭐

**重要**：DMS 使用 `config/dms.yaml` 中的 `primary.host` 和 `primary.port` 连接 InfluxDB，**不是** Docker 环境变量。

### 方案 A：使用外部 InfluxDB（推荐，如果已有 InfluxDB）

**适用场景**：已有 InfluxDB 服务运行在宿主机或其他服务器上（如你的 `172.19.202.120:8086`）

**配置步骤**：
1. 在 `config/dms.yaml` 中配置外部 InfluxDB：
   ```yaml
   primary:
     host: "172.19.202.120"  # 外部 InfluxDB IP 地址（推荐）
     # 或使用主机名（需要在 docker-compose.external-db.yml 中添加 extra_hosts 映射）
     # host: "host.docker.internal" 宿主机器
     port: 8086
     database: "dms_data"
   ```

2. 使用外部数据库配置启动：
   ```bash
   docker compose -f docker/docker-compose.external-db.yml up -d
   ```

3. 或者修改 `docker-compose.yml`：
   - 注释掉 `influxdb` 服务（整个服务块）
   - 注释掉 `dms` 的 `depends_on: influxdb`
   - 注释掉 `dms` 的 `networks` 部分
   - 取消注释 `extra_hosts`（如果需要访问宿主机服务）

**优点**：
- ✅ 复用现有数据库，数据集中管理
- ✅ 不占用额外资源
- ✅ 避免端口冲突

### 方案 B：使用 Docker 内的 InfluxDB

**适用场景**：独立部署，需要完整的 Docker 环境

**配置步骤**：
1. 在 `config/dms.yaml` 中配置 Docker 服务名或宿主机地址：
   ```yaml
   primary:
     host: "influxdb"  # Docker 服务名（同一网络内）
     # 或
     host: "host.docker.internal"  # Mac/Windows 访问宿主机
     # 或 Linux
     host: "172.17.0.1"  # Linux 宿主机 IP（Docker 默认网关）
     port: 8086
     database: "dms_data"
   ```

2. 使用默认配置启动：
   ```bash
   ./docker/docker_tools.sh up
   ```

**优点**：
- ✅ 一键部署，环境隔离
- ✅ 数据独立，易于迁移

### 端口冲突处理

如果 8086 端口被占用（如你的情况），可以：

1. **使用外部 InfluxDB**（推荐）：
   - 使用 `docker-compose.external-db.yml`
   - 或修改 `docker-compose.yml` 注释掉 `influxdb` 服务

2. **修改端口**：
   - 修改 `docker/.env` 中的 `INFLUXDB_PORT=18086`（或其他端口）
   - 在 `config/dms.yaml` 中相应修改 `primary.port`

3. **仅容器内访问**：
   - 注释掉 `influxdb` 服务的 `ports` 映射
   - 在 `config/dms.yaml` 中使用 `host: "influxdb"`

## 目录结构

```
sai/dms/
├── docker/              # Docker 配置文件目录
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── docker-compose.external-db.yml
│   ├── env.example
│   ├── .dockerignore
│   ├── docker_tools.sh
│   └── README.md
├── config/             # 配置文件（挂载到容器）
│   └── dms.yaml        # ⭐ InfluxDB 连接配置在这里
├── data/               # 数据目录（挂载到容器）
└── run/                # 运行时文件目录（挂载到容器）
    ├── logs/           # 日志文件
    ├── db/             # 数据库文件（SQLite）
    └── exports/        # 导出文件
```

## 注意事项

1. **构建上下文**：Docker 构建上下文是项目根目录（`..`），所以 Dockerfile 中的 `COPY` 命令是相对于项目根目录的。

2. **Volume 挂载**：所有 volumes 路径都是相对于 `docker-compose.yml` 文件位置的，使用 `../` 指向项目根目录。

3. **环境变量**：`.env` 文件应该放在 `docker/` 目录下，由 `docker_tools.sh` 自动管理。

4. **运行位置**：`docker_tools.sh` 脚本会自动检测项目根目录，但建议在项目根目录下运行。

5. **InfluxDB 配置**：DMS 从 `config/dms.yaml` 读取数据库配置，Docker 环境变量仅用于 Docker 内的 InfluxDB 初始化。
