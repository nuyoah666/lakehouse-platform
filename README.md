# 🏠 Lakehouse Platform

> **轻量级实时湖仓一体化开发平台** — 专为 Apache Flink + Apache Paimon + Apache Doris 技术栈打造的 Web IDE
>
> 🎯 **一句话**：像用 DBeaver 管理 MySQL 一样，在浏览器里管理你的实时湖仓全链路

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Flink](https://img.shields.io/badge/Flink-1.20-blue.svg)
![Paimon](https://img.shields.io/badge/Paimon-1.0-green.svg)
![Doris](https://img.shields.io/badge/Doris-4.0-orange.svg)
![Node.js](https://img.shields.io/badge/Node.js-18+-brightgreen.svg)
![Windows](https://img.shields.io/badge/Windows-11-0078D4?logo=windows)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](../../issues)

---

## 🎬 为什么做这个？

搭建 **Flink CDC → Paimon → Doris** 实时数仓时，你有没有这些痛点：

- ❌ Flink SQL Client 命令行写 SQL 没语法高亮、没提示、没历史
- ❌ 作业提交后要切到 Flink Web UI 查状态，来回跳转
- ❌ 暂停恢复要手动找 Savepoint 路径、拼命令行
- ❌ Catalog 元数据分散在各组件，没有一个统一入口
- ❌ 血缘关系靠脑记或 Excel，表多了根本理不清
- 🪟 **Windows 用户额外痛苦**：教程全假设你有 Linux 服务器，Docker 在 Windows 上慢如蜗牛，WSL 路径映射搞得人崩溃，踩坑全靠自己摸索

**Lakehouse Platform 把以上所有操作收敛到一个浏览器页面。** 零前端框架依赖，一个 `node server.js` 就跑起来。

---

## ✨ 功能亮点

| 模块 | 功能 |
|------|--------|
| **数据开发** | SQL 编辑器（行号、Catalog 树点击插入表名）、Flink 批/流双模式执行与作业提交、Checkpoint 配置、暂停/恢复（Savepoint 机制） |
| **Catalog 管理** | 8 种类型（Paimon/Hive/MySQL/PostgreSQL/Doris/MongoDB/Redis/Kafka），Type-routed Schema Discovery，支持 Flink + Doris 双引擎 |
| **作业管理** | Flink 作业列表 + 状态筛选、暂停（Savepoint + Cancel）、恢复（从 Savepoint 路径重提交）、血缘关系可视化 |
| **监控面板** | 集群状态卡片、Paimon 三栏浏览器、Doris 即席查询（三层模型 `catalog.database.table`）、Flink Web UI & Doris Web UI 嵌入式接入 |
| **血缘分析** | 提交时自动采集 source/sink 表、持久化存储、全状态展示（RUNNING/FINISHED/FAILED/CANCELED） |

### 🎁 核心亮点

- **🪟 Windows 原生部署**：无需 Linux 虚拟机或 Docker，Windows 11 + WSL2 直接跑全套 Flink/Paimon/Doris。后端 Node.js 跑在 Windows 本地，通过 WSL2 桥接调用 Flink CLI — 开箱即用
- **零依赖前端**：单文件 SPA（HTML+CSS+JS），无需 webpack/vite/npm build
- **统一 Catalog**：一套配置同时服务 Flink 和 Doris，自动生成双引擎 DDL
- **智能暂停/恢复**：一键 Savepoint → Cancel → 从断点续传，自动管理 Savepoint 生命周期
- **SQL 即作业**：写的 SQL 直接提交运行，脚本名 = 作业名，不用额外配置

> 💡 **为什么强调 Windows？**市面上几乎所有湖仓教程都假设你在 Linux 上操作。本平台从 Day 1 就在 Windows 上开发验证，路径映射、WSL 桥接、跨系统命令调用等坑全部踩通并封装好了。如果你是 Windows 用户（尤其是数据分析师/个人开发者），不用再折腾虚拟机了。

---

## 🏗 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                   Browser (前端 SPA)                     │
│  数据开发 │ Catalog │ 作业管理 │ 血缘 │ 监控 │ UI嵌入  │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP REST (:7071)
┌────────────────────▼────────────────────────────────────┐
│              Express Backend (Node.js)                   │
│  /api/flink/*  /api/doris/*  /api/paimon/*          │
│  /api/catalogs/*  /api/lineage/*                      │
└──────┬──────────────┬──────────────────────────────────┘
       │ WSL2 Bridge      │ MySQL Protocol
┌──────▼──────┐ ┌─────▼──────┐
│ Flink 1.20  │ │ Doris 4.0  │
│ (REST :8081)│ │ (MySQL :9130)│
│   WSL2 内    │ │   WSL2 内   │
└──────┬──────┘ └─────┬──────┘
       │              │
       └──────┬───────┘
              ▼
       ┌──────┐
       │Paimon│  Filesystem Catalog
       │Warehouse│ (共享存储)
       └──────┘
```

**典型数据流：** `MySQL → Flink CDC → Paimon（实时写入）→ Doris Catalog 查询`

> 📌 **Windows 部署架构**：Browser 和 Express Backend 都运行在 **Windows 本地**，Flink/Doris/Paimon 运行在 WSL2 Linux 环境中。Backend 通过 `wsl.exe` 桥接调用 Flink SQL Client，通过 MySQL 协议直连 Doris FE。对用户而言 — 全部透明，只需要浏览器。

### 项目结构

```
lakehouse-platform/
├── backend/
│   ├── server.js          # Express 后端（API 服务）
│   ├── package.json       # 后端依赖
│   ├── .env.example       # 环境变量模板 ← 复制为 .env 使用
│   └── .env               # 实际配置（不提交到 Git）
├── frontend/
│   └── index.html         # 单页应用（HTML/CSS/JS）
├── .gitignore             # Git 忽略规则
└── README.md              # 本文件
```

---

## 🚀 快速启动

### 🪟 Windows 用户（推荐）

本平台 **专为 Windows 环境开发**，无需 Docker 或 Linux 虚拟机：

| 前置条件 | 说明 |
|----------|------|
| Windows 11 + WSL2 | Ubuntu 20.04+，用于运行 Linux 版 Flink/Doris |
| Node.js ≥ 18 (Windows 版) | 后端直接跑在 Windows 本地 |
| Apache Flink 1.20.x (Linux) | 在 WSL2 内启动 Session 集群 |
| Apache Doris 4.0.x (Linux) | 在 WSL2 内部署 |
| Apache Paimon 1.0.x | 放入 `$FLINK_HOME/lib/` |

> **核心架构**：Node.js 后端运行在 **Windows 本地**（:7071），通过 WSL2 桥接调用 Flink CLI 和 Shell 命令。MySQL 若在 Windows 本地运行，用 `127.0.0.1:3306` 直连即可。

### 前置依赖（全平台）

| 组件 | 版本要求 | 说明 |
|------|----------|------|
| Node.js | ≥ 18 | 后端运行时（推荐 LTS） |
| WSL2 | Ubuntu 20.04+ | Linux 版 Flink 运行环境 |
| Apache Flink | 1.20.x | 需启动 Session 集群，REST API :8081 |
| Apache Doris | 4.0.x | FE 监听 MySQL 协议 :9130 |
| Apache Paimon | 1.0.x | 作为 Flink JAR 依赖（放入 `$FLINK_HOME/lib/`） |

### 安装与运行（3 步）

```bash
# 1️⃣ 克隆仓库
git clone <your-repo-url>.git
cd lakehouse-platform

# 2️⃣ 安装后端依赖
cd backend
npm install

# 3️⃣ 配置环境变量
cp .env.example .env
# 编辑 .env，按实际情况修改各组件地址和路径

# 4️⃣ 启动服务
npm start
# 或开发模式（文件变更自动重启）：npm run dev
```

浏览器访问：**http://localhost:7071**

> ⚠️ **启动前请确保 Flink 集群和 Doris 已就绪** — 平台启动时会自动检测各服务连通性。

---

## ⚙️ 环境变量配置

复制 `backend/.env.example` 为 `backend/.env`，按需修改：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PORT` | `7071` | 平台服务端口 |
| `FLINK_BASE_URL` | `http://localhost:8081` | Flink REST API 地址 |
| `FLINK_HOME` | `/mnt/g/flink/flink-1.20.4` | Flink 安装目录（WSL 内绝对路径） |
| `FLINK_SAVEPOINT_DIR` | `/mnt/g/flink/savepoints` | Savepoint 存储目录 |
| `DORIS_HOST` | `127.0.0.1` | Doris FE 地址 |
| `DORIS_PORT` | `9130` | Doris FE MySQL 协议端口 |
| `DORIS_USER` | `root` | Doris 用户名 |
| `DORIS_PASSWORD` | *(空)* | Doris 密码 |
| `PAIMON_WAREHOUSE` | `/mnt/g/paimon_warehouse` | Paimon Warehouse 路径（WSL 格式） |
| `DATA_DIR` | 系统临时目录 | 配置文件持久化存储目录 |
| `ALLOWED_ORIGINS` | *(自动)* | CORS 允许来源（逗号分隔） |

### Windows + WSL2 路径注意事项

- `FLINK_HOME`、`PAIMON_WAREHOUSE`、`FLINK_SAVEPOINT_DIR` 均使用 **WSL 内路径格式**（如 `/mnt/g/...`）
- Windows 的 `G:` 盘在 WSL 中映射为 `/mnt/g/`
- MySQL 若运行在 Windows 本地（非 WSL），连接地址用 `127.0.0.1:3306`
- Flink 必须是 **Linux 版本**（bin 目录含 `.sh` 脚本），在 WSL2 内启动

---

## 📖 使用指南

### 数据开发

1. **新建脚本**：左侧「+ 新建脚本」，输入名称
2. **编写 SQL**：编辑器支持行号显示；Catalog 树支持点击插入表名/字段名到光标处
3. **执行预览（▶ 执行）**：仅返回结果集，不提交作业。适合 SELECT/SHOW/DESCRIBE 等
4. **提交作业（⬆ 提交）**：真正提交到 Flink 集群运行。脚本名自动作为作业名
5. **暂停/恢复**：
   - 暂停 = 触发 Savepoint → 取消作业（Savepoint 路径自动保存）
   - 恢复 = 从 Savepoint 路径重提交 SQL（含 `SET execution.savepoint.path=...`）

### CDC 数据同步（MySQL → Paimon）

1. 在「Catalog」中创建 MySQL 类型 Catalog（填入 host/port/user/password/database）
2. 在数据开发中编写 Flink CDC DDL（或手动编写）
3. 确保包含 `execution.checkpointing.interval=30s`（必须！数据落盘依赖 Checkpoint）
4. 切换模式为 **Streaming**，点击「⬆ 提交作业」

### Catalog 导航

- 四层懒加载树：**Catalog → Database → Table → 字段**
- 支持 Flink 和 Doris 双引擎（通过 `engine` 字段区分适用场景）
- 提交作业时自动 prepend `CREATE CATALOG IF NOT EXISTS ...`（解决批作业 Catalog 丢失问题）
- 查询 Paimon 时自动 `REFRESH CATALOG`（解决元数据缓存问题）

---

## 🔧 API 接口

### Flink 作业管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/flink/overview` | 集群概况 |
| GET | `/api/flink/jobs` | 作业列表（合并 history + Flink API）|
| GET | `/api/flink/jobs/:id` | 作业详情（含 SQL 等元数据）|
| GET | `/api/flink/jobs/:id/plan` | 执行计划 |
| GET | `/api/flink/jobs/:id/checkpoints` | Checkpoint 列表 |
| GET | `/api/flink/jobs/:id/exceptions` | 异常记录 |
| POST | `/api/flink/execute` | 执行 SQL（返回结果集）|
| POST | `/api/flink/submit` | 提交 Flink 作业（批/流自动判断）|
| PATCH | `/api/flink/jobs/:id/pause` | 暂停（Savepoint + Cancel）|
| PATCH | `/api/flink/jobs/:id/resume` | 从 Savepoint 恢复 |
| PATCH | `/api/flink/jobs/:id/cancel` | 取消作业 |
| GET | `/api/flink/savepoints` | 已保存的 Savepoint 列表 |
| GET | `/api/flink/submit/log` | 最近一次提交日志 |

### Doris 查询引擎

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/doris/query` | 执行查询（SQL）|
| GET | `/api/doris/status` | 健康检查 |
| GET | `/api/doris/catalogs` | Catalog 列表 |
| GET | `/api/doris/databases` | 数据库列表 |
| GET | `/api/doris/tables` | 表列表 |

### Paimon 表浏览器

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/paimon/databases` | 数据库列表（通过 Doris）|
| GET | `/api/paimon/tables` | 表列表（通过 Doris）|
| GET | `/api/paimon/table-detail` | 表详情（Schema）|

### Catalog 管理（8 种类型）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/catalogs` | Catalog 列表 |
| POST | `/api/catalogs` | 创建 Catalog |
| PUT | `/api/catalogs/:id` | 更新 Catalog |
| DELETE | `/api/catalogs/:id` | 删除 Catalog |
| GET | `/api/catalogs/:id/databases` | 列出数据库 |
| GET | `/api/catalogs/:id/tables` | 列出表 |
| GET | `/api/catalogs/:id/schema` | 查看表 Schema |
| GET | `/api/catalogs/:id/preview` | 数据预览 |
| GET | `/api/catalogs/:id/ddl` | 生成 CREATE TABLE DDL |

### 血缘分析

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/lineage` | 血缘关系列表 |
| POST | `/api/lineage` | 手动录入血缘记录 |

---

## 🔒 安全特性

- **CORS 白名单**：严格限制允许的来源，不支持通配符绕过
- **SQL 注入防护**：所有 SQL 标识符经过正则校验（`validateIdentifier` + `safeId`），只允许合法字符
- **密码脱敏**：生成的 DDL 中密码字段自动替换为 `******`
- **命令注入防护**：WSL 命令执行过滤危险字符（`;|&$()\{\}\n\r`）
- **信息泄露防护**：健康检查接口仅暴露"已配置/未配置"状态，不暴露实际地址
- **全局错误处理**：`uncaughtException` / `UnhandledRejection` 捕获 + JSON 错误响应中间件

---

## 🛠 技术栈

| 层 | 技术 | 说明 |
|----|------|------|
| 前端 | HTML5 + CSS3 + Vanilla JS | 单文件 SPA，无框架依赖 |
| 后端 | Express.js 4.x | REST API，JSON 通信 |
| 流处理 | Apache Flink 1.20 | 通过 WSL2 调用 SQL Client |
| 湖仓存储 | Apache Paimon 1.0 | Filesystem Catalog |
| 查询引擎 | Apache Doris 4.0 | MySQL 协议兼容 |
| 数据同步 | Flink CDC 3.1 | MySQL binlog 实时捕获 |

---

## 🐛 常见问题

### Q: Flink SQL Client 启动很慢？
A: 正常现象。JVM 冷启动 + 优化器初始化需要 10~30 秒。流式提交默认等待 90 秒检测新作业。

### Q: CDC 数据没有写入 Paimon？
A: 确保开启了 Checkpoint（`execution.checkpointing.interval=30s`）。Paimon 的 Commit 操作依赖 Checkpoint 触发，不开启则数据停留在 Writer Buffer。

### Q: Doris 查询 Paimon 数据是旧的？
A: 这是 Doris 元数据缓存问题。平台已内置自动 `REFRESH CATALOG` 逻辑。若仍有问题，可在 Doris 中手动执行 `REFRESH CATALOG paimon_lake;`。

### Q: WSL 内存不足？
A: Flink TM + Doris BE 同时运行会占用大量内存。建议：
- 关闭不需要的服务
- 调整 `.wslconfig` 增加内存限制
- 及时取消不再需要的流式作业

### Q: 如何确认 Flink 集群状态？
A: 访问 http://localhost:8081 或使用平台内嵌的 Flink UI。

---

## 📋 路线图

- [x] MVP：SQL IDE + 作业提交 + Catalog 导航 + 监控面板
- [x] Catalog 管理（8 种类型）+ Type-routed Schema Discovery
- [x] 暂停/恢复（Savepoint 机制）
- [x] 血缘关系采集与可视化
- [ ] CodeMirror 6 集成（SQL 语法高亮 + 格式化）
- [ ] SSE 实时推送执行进度
- [ ] Docker Compose 一键部署
- [ ] 多用户认证与权限控制
- [ ] SQL 审计日志

---

## 🤝 Contributing

欢迎 PR 和 Issue！提交前请确保：

1. 代码风格与现有代码一致
2. 涉及后端改动需更新 `backend/.env.example`
3. 涉及 API 变更需更新本 README 的接口表
4. 安全相关改动需说明影响范围

---

## 📄 License

[MIT](LICENSE)
