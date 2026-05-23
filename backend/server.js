const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const path = require('path');
const { execSync, exec } = require('child_process');
const fs = require('fs');
const os = require('os');

// 加载 .env 配置（优先）
try { require('dotenv').config(); } catch (e) {
  console.warn('[CONFIG] dotenv 不可用，将使用默认配置和环境变量');
}

const app = express();

// CORS：生产环境严格限制来源，开发环境允许 localhost
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : ['http://localhost:7071', 'http://127.0.0.1:7071'];
app.use(cors({
  origin: (origin, cb) => {
    // 不带 origin 的请求（如 Postman/curl）在非生产环境放行
    if (!origin) return cb(null, true);
    if (allowedOrigins.includes(origin)) return cb(null, true);
    // 开发环境下允许同端口请求
    const devPort = process.env.PORT || 7071;
    if (origin === `http://localhost:${devPort}` || origin === `http://127.0.0.1:${devPort}`) return cb(null, true);
    console.warn(`[CORS] Blocked origin: ${origin}`);
    cb(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));

app.use(express.json({ limit: '10mb' }));
// 确保 Express 正确处理 UTF-8
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// 静态前端文件
app.use(express.static(path.join(__dirname, '../frontend')));

// ==================== 工具函数 ====================
// SQL 标识符校验（防止注入：只允许字母、数字、下划线、连字符）
const validateIdentifier = (name, field = 'identifier') => {
  if (!name || typeof name !== 'string') return false;
  // 只允许 ASCII 字母、数字、下划线、连字符，且不以数字/连字符开头
  return /^[a-zA-Z_][a-zA-Z0-9_\-]*$/.test(name);
};
// 安全拼接 SQL 标识符（带校验和转义）
const safeId = (name, field = 'identifier') => {
  if (!validateIdentifier(name, field)) {
    const err = new Error(`Invalid ${field}: ${name}`);
    err.code = 'INVALID_IDENTIFIER';
    throw err;
  }
  return `\`${name}\``;
};
const CONFIG = {
  port: parseInt(process.env.PORT) || 7070,
  flink: {
    baseUrl: process.env.FLINK_BASE_URL || 'http://localhost:8081',
    home: process.env.FLINK_HOME || '/mnt/g/flink/flink-1.20.4',
    version: '1.20.4',
    savepointDir: process.env.FLINK_SAVEPOINT_DIR || '/mnt/g/flink/savepoints',
  },
  doris: {
    host: process.env.DORIS_HOST || '127.0.0.1',
    port: parseInt(process.env.DORIS_PORT) || 9130,
    user: process.env.DORIS_USER || 'root',
    password: process.env.DORIS_PASSWORD || '',
  },
  paimon: {
    warehouse: process.env.PAIMON_WAREHOUSE || '/mnt/g/paimon_warehouse',
  },
  // 持久化目录（默认系统临时目录，可通过环境变量覆盖）
  dataDir: process.env.DATA_DIR || os.tmpdir(),
};

// ==================== Flink 资源配置持久化 ====================
const FLINK_RESOURCE_STORE = path.join(CONFIG.dataDir, 'lakehouse_flink_resources.json');

const DEFAULT_FLINK_RESOURCES = {
  jobManager: { memory: '1024mb', cpu: 1 },
  taskManager: { memory: '1024mb', cpu: 1, slots: 2 },
  parallelism: 1,
};

const loadFlinkResources = () => {
  try { return JSON.parse(fs.readFileSync(FLINK_RESOURCE_STORE, 'utf8')); }
  catch (e) { return { ...DEFAULT_FLINK_RESOURCES }; }
};
const saveFlinkResources = (data) => {
  fs.writeFileSync(FLINK_RESOURCE_STORE, JSON.stringify(data, null, 2), 'utf8');
};

// 解析 Flink config.yaml 提取当前实际资源值（只读参考）
const parseFlinkConfig = () => {
  try {
    // 将 Windows 路径转为 WSL 路径（G:\xxx → /mnt/g/xxx）
    const winPath = path.join(CONFIG.flink.home, 'conf/config.yaml');
    const wslPath = winPath.replace(/\\/g, '/').replace(/^([A-Z]):\//i, '/mnt/$1/');
    const raw = execSync(`wsl.exe -e bash -c "cat '${wslPath}'"`, { encoding: 'utf8', timeout: 5000 });
    // 用宽松正则提取关键值（跳过注释行）
    const jmMem = (raw.match(/size:\s*(\d+[mMgGkK])/) || [])[1] || '?'; // 取第一个 size 值 (JM)
    const lines = raw.split('\n');
    let tmMem = '?';
    // 找 taskmanager 下第二个 size（第一个是 JM 的）
    const allSizes = [...raw.matchAll(/size:\s*(\d+[mMgGkK])/g)];
    if (allSizes.length >= 2) tmMem = allSizes[1][1];
    const tmSlots = (raw.match(/numberOfTaskSlots:\s*(\S+)/) || [])[1] || '?';
    // parallelism.default 前面可能有注释行，用宽松匹配
    const parMatch = raw.match(/parallelism:\s*\n(?:\s*#.*\n)*\s*default:\s*(\d+)/);
    const parallelism = (parMatch || [])[1] || '?';
    return { jmMemory: allSizes[0]?.[1] || '?', tmMemory: tmMem, tmSlots, parallelism };
  } catch (e) {
    return { error: e.message };
  }
};

// ==================== 日志工具函数 ====================
const logError = (tag, msg) => {
  const ts = new Date().toISOString();
  console.error(`[ERROR][${ts}][${tag}] ${msg}`);
  // 可选：写入日志文件（调试时有用）
  try { fs.appendFileSync(path.join(CONFIG.dataDir, 'lakehouse_debug.log'), `${ts}\t[ERROR]\t[${tag}]\t${msg}\n`, 'utf8'); } catch {}
};
const logInfo = (tag, msg) => {
  const ts = new Date().toISOString();
  console.log(`[INFO ][${ts}][${tag}] ${msg}`);
  try { fs.appendFileSync(path.join(CONFIG.dataDir, 'lakehouse_debug.log'), `${ts}\t[INFO ]\t[${tag}]\t${msg}\n`, 'utf8'); } catch {}
};

// ==================== 血缘持久化存储（Task #124）====================
// 持久化血缘记录，支持：提交自动采集、删除级联、全状态展示
const LINEAGE_STORE = path.join(CONFIG.dataDir, 'lakehouse_lineage.json');

let _lineageCache = null;
const getLineageStore = () => {
  if (_lineageCache) return _lineageCache;
  try { return (_lineageCache = JSON.parse(fs.readFileSync(LINEAGE_STORE, 'utf8'))); }
  catch (e) { return (_lineageCache = { records: [] }); }
};
const saveLineageStore = (data) => {
  _lineageCache = data;
  try { fs.writeFileSync(LINEAGE_STORE, JSON.stringify(data, null, 2), 'utf8'); }
  catch (e) { logError('save-lineage-store', e.message); }
};

// 血缘记录 CRUD 辅助
const lineageAddRecord = (record) => {
  const store = getLineageStore();
  // 防重复（同一 batchId 不重复添加）
  if (record.batchId && store.records.some(r => r.batchId === record.batchId)) {
    // 更新已有记录
    const idx = store.records.findIndex(r => r.batchId === record.batchId);
    if (idx >= 0) store.records[idx] = { ...store.records[idx], ...record, updatedAt: new Date().toISOString() };
  } else {
    record.createdAt = record.createdAt || new Date().toISOString();
    record.updatedAt = new Date().toISOString();
    store.records.push(record);
  }
  saveLineageStore(store);
};

const lineageDeleteByJobId = (jobId) => {
  const store = getLineageStore();
  const beforeLen = store.records.length;
  // 支持按 jobId 或 batchId 删除
  store.records = store.records.filter(r =>
    r.jobId !== jobId && r.batchId !== jobId &&
    (!r.flinkJobIds || !r.flinkJobIds.includes(jobId))
  );
  if (store.records.length < beforeLen) saveLineageStore(store);
  return beforeLen - store.records.length; // 返回删除条数
};

// ==================== Catalog 持久化 ====================
// 统一 Catalog 架构：合并 DataSource + 原 Catalog 为单一 Catalog 抽象
// 支持类型：paimon, hive, mysql, postgres, doris, mongodb, redis, kafka
const CATALOG_STORE = path.join(CONFIG.dataDir, 'lakehouse_catalogs.json');

// Catalog 类型分类
const CATALOG_TYPES = {
  // 湖存储（文件系统/Metastore）
  lake: ['paimon', 'hive'],
  // 关系型数据库（JDBC）
  jdbc: ['mysql', 'postgres', 'doris'],
  // NoSQL / KV / 消息队列
  nosql: ['mongodb', 'redis', 'kafka'],
};
const ALL_CATALOG_TYPES = [...CATALOG_TYPES.lake, ...CATALOG_TYPES.jdbc, ...CATALOG_TYPES.nosql];

// ★ Task #124: 从 SQL 文本提取 Source/Sink 表名（用于血缘自动采集）
const extractSourceTables = (sql) => {
  if (!sql) return [];
  const tables = [];
  // 匹配: FROM table_name, FROM db.table, JOIN table_name, IN table_name
  // 以及 CREATE TABLE ... WITH ('connector' = 'mysql-cdc', 'table-name' = 'xxx')
  const fromJoinRegex = /\bFROM\s+(`?[\w_]+`?\.)?`?(\w+)`?|\bJOIN\s+(`?[\w_]+`?\.)?`?(\w+)`?/gi;
  let m;
  while ((m = fromJoinRegex.exec(sql)) !== null) {
    const t = (m[2] || m[4] || '').trim();
    if (t && !t.toUpperCase().startsWith('VALUES') && tables.indexOf(t) < 0) tables.push(t);
  }
  // CDC connector 表名
  const cdcTableRegex = /'table-name'\s*=\s*'([^']+)'/gi;
  while ((m = cdcTableRegex.exec(sql)) !== null) {
    if (tables.indexOf(m[1]) < 0) tables.push(m[1]);
  }
  return tables;
};
const extractSinkTables = (sql) => {
  if (!sql) return [];
  const tables = [];
  // 匹配: INSERT INTO table_name, INSERT INTO db.table, CREATE TABLE sink_name AS
  const insertCreateRegex = /\bINSERT\s+(?:OVERWRITE\s+)?INTO\s+(`?[\w_]+`?\.)?`?(\w+)`?|\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?(?:\s+AS|\s*\()/gi;
  let m;
  while ((m = insertCreateRegex.exec(sql)) !== null) {
    const t = (m[2] || m[3] || '').trim();
    if (t && tables.indexOf(t) < 0 && !['VALUES'].includes(t.toUpperCase())) tables.push(t);
  }
  // Paimon Sink 通常在 Writer 中，也尝试从 CREATE TABLE ... WITH paimon 提取
  const createWithRegex = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\([^)]*\)\s*WITH\s*\([^)]*paimon/gi;
  while ((m = createWithRegex.exec(sql)) !== null) {
    if (tables.indexOf(m[1]) < 0) tables.push(m[1]);
  }
  return tables;
};

const loadCatalogs = () => {
  try { return JSON.parse(fs.readFileSync(CATALOG_STORE, 'utf8')); }
  catch (e) {
    // 初始化默认 Paimon Catalog
    const defaults = {
      paimon_lake: {
        id: 'paimon_lake',
        name: 'Paimon Lake',
        type: 'paimon',
        engine: 'both', // flink | doris | both
        props: {
          paimon_catalog_type: 'filesystem',
          warehouse: CONFIG.paimon.warehouse,
        },
        dorisProps: {
          warehouse: 'file://' + CONFIG.paimon.warehouse,
        },
        createdAt: new Date().toISOString(),
      }
    };
    fs.writeFileSync(CATALOG_STORE, JSON.stringify(defaults, null, 2));
    return defaults;
  }
};
const saveCatalogs = (data) => {
  fs.writeFileSync(CATALOG_STORE, JSON.stringify(data, null, 2));
  catalogCache = data;
};

// 内存缓存，启动时加载
let catalogCache = null;
const getCatalogs = () => catalogCache || loadCatalogs();

// 启动时加载 Catalog
loadCatalogs();

// ==================== SQL Store ====================
const SQL_STORE = path.join(CONFIG.dataDir, 'lakehouse_sql_store.json');

// SQL Store 格式: { byTime: { timestamp: sql }, byJob: { jobName: sql } }
const saveSqlToStore = (sql, jobName) => {
  try {
    let store = { byTime: {}, byJob: {} };
    try { store = JSON.parse(fs.readFileSync(SQL_STORE, 'utf8')); } catch (e) {}
    // 按时间存（最近 50 条）
    const key = String(Date.now());
    store.byTime = store.byTime || {};
    store.byTime[key] = sql;
    const keys = Object.keys(store.byTime).sort();
    if (keys.length > 50) {
      keys.slice(0, keys.length - 50).forEach(k => delete store.byTime[k]);
    }
    // 按作业名存（恢复时用）
    if (jobName) {
      store.byJob = store.byJob || {};
      store.byJob[jobName] = sql;
    }
    fs.writeFileSync(SQL_STORE, JSON.stringify(store, null, 2));
    return key;
  } catch (e) { return null; }
};

const getSqlFromStore = (jobName) => {
  try {
    const raw = JSON.parse(fs.readFileSync(SQL_STORE, 'utf8'));
    // 兼容旧格式：扁平键值（timestamp -> sql）
    const store = raw.byTime ? raw : { byTime: raw, byJob: {} };
    const nameRe = jobName
      ? new RegExp(`SET\\s+'pipeline\\.name'\\s*=\\s*['"']${jobName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}['"']`, 'i')
      : null;

    // 收集所有 SQL 条目（byJob + byTime + 旧格式扁平键）
    const allSqls = [];
    if (store.byJob) allSqls.push(...Object.values(store.byJob));
    if (store.byTime) allSqls.push(...Object.values(store.byTime));
    // 旧格式：扁平时间戳键
    for (const [k, v] of Object.entries(raw)) {
      if (k !== 'byJob' && k !== 'byTime' && typeof v === 'string') allSqls.push(v);
    }

    // 按作业名精确匹配：找包含对应 pipeline.name 的 SQL
    if (nameRe) {
      for (const sql of allSqls) {
        if (nameRe.test(sql)) return sql;
      }
    }
    // fallback 到最新一条
    const keys = Object.keys(store.byTime || {}).sort().reverse();
    return keys.length > 0 ? store.byTime[keys[0]] : null;
  } catch (e) { return null; }
};

// ==================== Job History Store ====================
const JOB_HISTORY_STORE = path.join(CONFIG.dataDir, 'lakehouse_job_history.json');
const JOB_DELETED_STORE = path.join(CONFIG.dataDir, 'lakehouse_job_deleted.json');

const loadJobHistory = () => {
  try { return JSON.parse(fs.readFileSync(JOB_HISTORY_STORE, 'utf8')); }
  catch (e) { return {}; }
};
const saveJobHistory = (data) => {
  fs.writeFileSync(JOB_HISTORY_STORE, JSON.stringify(data, null, 2));
};

// 已删除作业黑名单（用户主动删除历史的作业 ID 集合）
const loadDeletedJobs = () => {
  try { return JSON.parse(fs.readFileSync(JOB_DELETED_STORE, 'utf8')); }
  catch (e) { return []; }
};
const saveDeletedJobs = (list) => {
  fs.writeFileSync(JOB_DELETED_STORE, JSON.stringify(list, null, 2));
};

// 内存缓存，启动时加载
let jobHistoryCache = null;
const getJobHistory = () => jobHistoryCache || (jobHistoryCache = loadJobHistory());
let deletedJobsCache = null;
const getDeletedJobs = () => deletedJobsCache || (deletedJobsCache = loadDeletedJobs());

// ==================== Catalog API ====================
// Catalog 列表（可按 engine 或 type 过滤）
app.get('/api/catalogs', (req, res) => {
  const { engine, type } = req.query;
  const all = getCatalogs();
  let cats = Object.values(all);
  if (engine) cats = cats.filter(c => c.engine === engine || c.engine === 'both');
  if (type) cats = cats.filter(c => c.type === type);
  res.json({ success: true, data: cats.map(c => ({
    id: c.id, name: c.name, type: c.type, engine: c.engine,
    props: c.props, dorisProps: c.dorisProps,
    // JDBC 类型的连接信息（脱敏密码）
    connection: c.connection ? { ...c.connection, password: c.password ? '******' : undefined } : undefined,
    createdAt: c.createdAt
  })) });
});

// 支持的 Catalog 类型列表
app.get('/api/catalogs/_types', (_req, res) => {
  res.json({
    success: true,
    data: ALL_CATALOG_TYPES.map(t => ({
      value: t,
      category: CATALOG_TYPES.lake.includes(t) ? 'lake' : CATALOG_TYPES.jdbc.includes(t) ? 'jdbc' : 'nosql',
      label: { paimon:'Paimon', hive:'Hive Metastore', mysql:'MySQL', postgres:'PostgreSQL', doris:'Doris/StarRocks', mongodb:'MongoDB', redis:'Redis', kafka:'Kafka' }[t] || t,
    }))
  });
});

// Catalog 详情（含完整 connection 信息，用于编辑回填）
app.get('/api/catalogs/:id', (req, res) => {
  const c = getCatalogs()[req.params.id];
  if (!c) return res.json({ success: false, error: 'Catalog 不存在' });
  res.json({ success: true, data: c });
});

// 创建/更新 Catalog（统一数据模型：支持 lake + jdbc + nosql）
app.post('/api/catalogs', (req, res) => {
  let { id, name, type, engine, props, dorisProps, connection } = req.body;
  if (!id || !name || !type) return res.json({ success: false, error: 'id、name、type 必填' });
  if (!ALL_CATALOG_TYPES.includes(type)) return res.json({ success: false, error: `不支持的类型: ${type}，可选: ${ALL_CATALOG_TYPES.join(', ')}` });

  // JDBC 类型必须提供 connection 信息
  const isJdbc = CATALOG_TYPES.jdbc.includes(type);
  if (isJdbc && (!connection || !connection.host)) {
    return res.json({ success: false, error: 'JDBC 类型（MySQL/PostgreSQL/Doris）必须提供连接信息（host）' });
  }

  // 合并 connection（保留密码等敏感信息）
  const all = getCatalogs();
  const existing = all[id];
  const conn = isJdbc ? {
    host: connection.host,
    port: parseInt(connection.port) || (type === 'postgres' ? 5432 : type === 'doris' ? 9030 : 3306),
    user: connection.user || 'root',
    // 密码：更新时若前端传了就用新的，否则保留旧值（避免清空）
    password: connection.password !== undefined ? connection.password : (existing?.connection?.password || ''),
    database: connection.database || '',
  } : undefined;

  all[id] = {
    id, name, type,
    engine: engine || (isJdbc ? 'flink' : 'both'),
    props: props || {},
    dorisProps: dorisProps || {},
    ...(conn ? { connection: { ...conn, password: conn.password }, /* 原始密码用于后端操作 */ _password: conn.password } : {}),
    createdAt: existing?.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  saveCatalogs(all);
  // 返回时脱敏密码
  const ret = { ...all[id] };
  if (ret._password) { ret.connection.password = '******'; delete ret._password; }
  res.json({ success: true, data: ret });
});

// 删除 Catalog
app.delete('/api/catalogs/:id', (req, res) => {
  const all = getCatalogs();
  if (!all[req.params.id]) return res.json({ success: false, error: 'Catalog 不存在' });
  delete all[req.params.id];
  saveCatalogs(all);
  res.json({ success: true });
});

// 测试 Catalog 连接（type-routed）
app.post('/api/catalogs/:id/test', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });

  try {
    if (cat.type === 'paimon') {
      // Paimon: 通过 Doris 测试
      let conn;
      try {
        conn = await getDorisConn();
        const dorisProps = cat.dorisProps || cat.props || {};
        const warehouse = dorisProps.warehouse || `file://${dorisProps.warehouse_path || '/mnt/g/paimon_warehouse'}`;
        const paimonCatType = dorisProps.paimon_catalog_type || 'filesystem';
        if (!validateIdentifier(cat.id, 'catalog id')) return res.json({ success: false, error: 'Catalog ID 包含非法字符' });
        await conn.query(`CREATE CATALOG IF NOT EXISTS ${safeId(cat.id)} PROPERTIES ('type'='paimon','paimon.catalog.type'='${paimonCatType}','warehouse'='${warehouse}')`);
        await conn.query(`REFRESH CATALOG ${safeId(cat.id)}`);
        res.json({ success: true, message: 'Paimon Catalog 连接正常（通过 Doris 验证）' });
      } finally { if (conn) conn.end(); }
    } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      // JDBC 类型：直接建连测试
      const c = cat.connection || {};
      // Doris/StarRocks 用 MySQL 协议
      const isMysqlProtocol = ['mysql', 'doris'].includes(cat.type);
      if (isMysqlProtocol) {
        let conn;
        try {
          conn = await mysql.createConnection({
            host: c.host, port: c.port || 3306,
            user: c.user, password: cat._password || c.password || '',
            database: c.database || undefined, connectTimeout: 5000
          });
          const [rows] = await conn.query('SELECT 1');
          res.json({ success: true, message: `${cat.type.toUpperCase()} 连接成功` });
        } finally { if (conn) conn.end(); }
      } else {
        // PostgreSQL 等非 MySQL 协议 — 暂时返回提示（后续 Phase 6 加 pg 驱动）
        res.json({ success: true, message: `${cat.type} 配置已保存（需安装对应驱动后可测试）`, warning: 'no_driver' });
      }
    } else {
      res.json({ success: true, message: `${cat.type} Catalog 配置有效`, warning: 'manual_test' });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ==================== 统一 Catalog 元数据发现 API（type-routed） ====================
// 所有类型的元数据都通过统一的 URL 路径访问，后端根据 cat.type 路由到不同的发现策略

// 获取 JDBC 类型的 MySQL 连接（复用工具函数）
const getJdbcConnection = (cat) => {
  const c = cat.connection || {};
  return mysql.createConnection({
    host: c.host, port: c.port || 3306,
    user: c.user, password: cat._password || c.password || '',
    database: c.database || undefined, connectTimeout: 8000
  });
};

// Database 列表
app.get('/api/catalogs/:id/databases', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });

  try {
    if (cat.type === 'paimon') {
      // Paimon: 列出 warehouse 下的 *.db 目录
      const output = runWsl(`ls ${CONFIG.paimon.warehouse}/ 2>/dev/null`);
      const dbs = output.split('\n').filter(d => d.trim().endsWith('.db')).map(d => d.trim().replace(/\.db$/, ''));
      res.json({ success: true, data: dbs });
    } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      // JDBC: 如果配置了具体数据库则只返回该库，否则 SHOW DATABASES
      const conn = await getJdbcConnection(cat);
      try {
        const configuredDb = cat.connection?.database;
        if (configuredDb) {
          // 验证配置的数据库是否存在
          const [rows] = await conn.query('SELECT DATABASE()');
          const currentDb = rows[0] && Object.values(rows[0])[0];
          res.json({ success: true, data: [configuredDb], note: `当前连接数据库：${configuredDb}` });
        } else {
          const [rows] = await conn.query('SHOW DATABASES');
          res.json({ success: true, data: rows.map(r => Object.values(r)[0]).filter(db => !['information_schema', 'performance_schema', 'mysql', 'sys'].includes(db.toLowerCase())) });
        }
      } finally { conn.end(); }
    } else if (cat.type === 'hive') {
      // Hive: 通过 Doris 或 metastore（暂未实现，返回空）
      res.json({ success: true, data: [], note: 'Hive 类型需配置 Metastore URI' });
    } else {
      res.json({ success: true, data: [], note: `${cat.type} 类型暂不支持数据库列表` });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Table 列表
app.get('/api/catalogs/:id/tables', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });
  const { database } = req.query;

  try {
    if (cat.type === 'paimon') {
      if (!database) return res.json({ success: false, error: 'database 参数必填' });
      const dbPath = `${CONFIG.paimon.warehouse}/${database}.db`;
      const output = runWsl(`ls ${dbPath}/ 2>/dev/null`);
      const tables = output.split('\n').filter(Boolean);
      res.json({ success: true, data: tables });
    } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      const conn = await getJdbcConnection(cat);
      try {
        const db = database || cat.connection?.database;
        if (!db) return res.json({ success: false, error: '未指定 database' });
        const [rows] = await conn.query(`SELECT TABLE_NAME as name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?`, [db]);
        res.json({ success: true, data: rows.map(r => r.name) });
      } finally { conn.end(); }
    } else {
      res.json({ success: true, data: [], note: `${cat.type} 暂不支持` });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 表 Schema（字段详情）
app.get('/api/catalogs/:id/tables/:table/schema', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });
  const { database } = req.query;
  const table = req.params.table;

  try {
    if (cat.type === 'paimon') {
      // Paimon: 读 schema-0 文件
      safeId(database, 'database'); safeId(table, 'table');
      const tablePath = `${CONFIG.paimon.warehouse}/${database}.db/${table}`;
      const schemaRaw = runWsl(`cat ${tablePath}/schema/schema-0 2>/dev/null || echo '{}'`);
      let schema = {};
      try { schema = JSON.parse(schemaRaw); } catch (e) { schema = { raw: schemaRaw }; }
      const fields = schema.fields || [];
      const pk = schema.primaryKeys || [];
      res.json({
        success: true,
        data: {
          columns: fields.map(f => ({ name: f.name, type: f.type, nullable: !pk.includes(f.name), pk: pk.includes(f.name), comment: f.comment || '' })),
          primaryKey: pk,
          source: 'paimon-schema',
        }
      });
    } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      // JDBC: INFORMATION_SCHEMA.COLUMNS
      const conn = await getJdbcConnection(cat);
      try {
        const db = database || cat.connection?.database;
        const [rows] = await conn.query(
          `SELECT COLUMN_NAME as name, DATA_TYPE as type, IS_NULLABLE as nullable, COLUMN_KEY as colKey, COLUMN_COMMENT as comment, EXTRA as extra
           FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
          [db, table]
        );
        const pk = rows.filter(r => r.colKey === 'PRI').map(r => r.name);
        res.json({
          success: true,
          data: {
            columns: rows.map(r => ({ name: r.name, type: r.type, nullable: r.nullable === 'YES', pk: r.colKey === 'PRI', comment: r.comment || '', auto: r.extra?.includes('auto_increment') })),
            primaryKey: pk,
            source: 'information_schema',
          }
        });
      } finally { conn.end(); }
    } else {
      res.json({ success: false, error: `${cat.type} 暂不支持 Schema 发现` });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 数据预览
app.get('/api/catalogs/:id/tables/:table/preview', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });
  const { database } = req.query;
  const table = req.params.table;
  const limit = parseInt(req.query.limit) || 50;

  try {
    if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      const conn = await getJdbcConnection(cat);
      try {
        const db = database || cat.connection?.database;
        const [rows, fields] = await conn.query(`SELECT * FROM \`${db}\`.\`${table}\` LIMIT ?`, [limit]);
        const columns = fields.map(f => f.name);
        const rowList = rows.map(r => ({ ...r }));
        res.json({ success: true, data: { columns, rows: rowList }, rowCount: rows.length });
      } finally { conn.end(); }
    } else if (cat.type === 'paimon') {
      // Paimon 通过 Flink SQL 执行预览：构建完整 SQL（含 USE CATALOG + USE db）
      const db = database || 'default';
      const catalogId = cat.id;
      const fullSql = [
        `USE CATALOG ${catalogId};`,
        `USE ${db};`,
        `SELECT * FROM \`${table}\` LIMIT ${limit};`
      ].join('\n');

      // 内联执行（复用 /api/flink/execute 的核心逻辑）
      const execTs = Date.now();
      const tmpFile = `/tmp/lakehouse_preview_${execTs}.sql`;
      const outFile = `/tmp/lakehouse_preview_${execTs}.out`;

      const catalogs = getCatalogs();
      const flinkCats = Object.values(catalogs).filter(c => c.engine === 'flink' || c.engine === 'both');
      const catalogStmts = flinkCats.map(c => {
        if (c.type === 'paimon') {
          const w = c.props.warehouse || '/mnt/g/paimon_warehouse';
          const wPath = w.startsWith('/') ? w : '/' + w;
          return `CREATE CATALOG IF NOT EXISTS ${c.id} WITH (\n  'type' = 'paimon',\n  'paimon.catalog.type' = '${c.props.paimon_catalog_type || 'filesystem'}',\n  'warehouse' = 'file://${wPath}'\n);`;
        }
        return '';
      }).filter(Boolean).join('\n');

      const execSql = `SET 'sql-client.execution.result-mode' = 'TABLEAU';\nSET 'execution.runtime-mode' = 'BATCH';\n${catalogStmts}\n${fullSql}`;
      const b64 = Buffer.from(execSql).toString('base64');
      execSync(`wsl.exe -e bash -c "echo '${b64}' | base64 -d > ${tmpFile} && chmod 644 ${tmpFile}"`);
      const child = exec(`wsl.exe -e bash -c "cd ${CONFIG.flink.home} && bash bin/sql-client.sh -f ${tmpFile} > ${outFile} 2>&1; echo '==PREVIEW_DONE==' >> ${outFile}"`);
      child.on('error', (err) => { console.error('Flink preview error:', err.message); });

      // 等待执行结果
      let output = '';
      let done = false;
      for (let i = 0; i < 60; i++) {
        await new Promise(r => setTimeout(r, 1000));
        try {
          const out = execSync(`wsl.exe -e bash -c "cat ${outFile} 2>/dev/null || echo ''"`, { timeout: 5000 }).toString();
          if (out.includes('==PREVIEW_DONE==')) {
            output = out.replace('==PREVIEW_DONE==', '').trim();
            done = true;
            break;
          }
          output = out;
        } catch (e) {}
      }
      try { exec(`wsl.exe -e bash -c "rm -f ${tmpFile} ${outFile}"`, () => {}); } catch (e) {}

      // 解析 TABLEAU 格式输出为结构化数据
      if (!done && !output) {
        return res.json({ success: false, error: '预览超时（60秒）或 Flink SQL Client 未响应' });
      }

      // 解析 TABLEAU 表格
      const lines = output.split('\n').filter(l => l.trim().startsWith('|'));
      if (lines.length < 2) {
        return res.json({ success: false, error: '无数据或查询结果为空', rawOutput: output.substring(0, 500) });
      }
      const cols = lines[0].split('|').slice(1, -1).map(h => h.trim()).filter(Boolean);
      const rows = [];
      for (let i = 2; i < lines.length; i++) {
        const cells = lines[i].split('|').slice(1, -1).map(c => c.trim());
        const row = {};
        cols.forEach((col, idx) => { row[col] = cells[idx] || null; });
        rows.push(row);
      }
      return res.json({ success: true, data: { columns: cols, rows }, rowCount: rows.length });
    } else {
      res.json({ success: false, error: `${cat.type} 暂不支持数据预览` });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 生成 Flink DDL
app.get('/api/catalogs/:id/tables/:table/ddl', async (req, res) => {
  const cat = getCatalogs()[req.params.id];
  if (!cat) return res.json({ success: false, error: 'Catalog 不存在' });
  const { database } = req.query;
  const table = req.params.table;

  try {
    // 先获取 Schema
    const schemaRes = await new Promise((resolve) => {
      // 内部调用：模拟请求获取 schema
      const mockReq = { params: { id: cat.id, table }, query: { database } };
      const mockRes = {
        json: (data) => resolve(data),
        statusCode: 200,
        setHeader: () => mockRes,
      };
      // 直接调用 handler 逻辑（避免 HTTP 循环）
      (async () => {
        try {
          if (cat.type === 'paimon') {
            safeId(database, 'database'); safeId(table, 'table');
            const tablePath = `${CONFIG.paimon.warehouse}/${database}.db/${table}`;
            const schemaRaw = runWsl(`cat ${tablePath}/schema/schema-0 2>/dev/null || echo '{}'`);
            let schema = {}; try { schema = JSON.parse(schemaRaw); } catch (e) { schema = { raw: schemaRaw }; }
            const fields = schema.fields || [];
            const pk = schema.primaryKeys || [];
            mockRes.json({ success: true, data: { columns: fields.map(f => ({ name:f.name, type:f.type, comment:f.comment||'', pk:pk.includes(f.name) })), primaryKey: pk } });
          } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
            const conn = await getJdbcConnection(cat);
            try {
              const db = database || cat.connection?.database;
              const [rows] = await conn.query(
                `SELECT COLUMN_NAME as name, DATA_TYPE as type, IS_NULLABLE as nullable, COLUMN_KEY as colKey, COLUMN_COMMENT as comment, CHARACTER_MAXIMUM_LENGTH as maxLen, NUMERIC_PRECISION as numPrec, NUMERIC_SCALE as numScale
                 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
                [db, table]
              );
              const pk = rows.filter(r => r.colKey === 'PRI').map(r => r.name);
              mockRes.json({ success: true, data: { columns: rows.map(r => ({ name:r.name, type:r.type, nullable:r.nullable==='YES', pk:r.colKey==='PRI', comment:r.comment||'' })), primaryKey: pk } });
            } finally { conn.end(); }
          } else {
            mockRes.json({ success: false, error: '不支持的类型' });
          }
        } catch (e) {
          mockRes.json({ success: false, error: e.message });
        }
      })();
    });

    // 等待 schema 结果（简化：直接内联逻辑）
    let schemaData;
    if (cat.type === 'paimon') {
      safeId(database, 'database'); safeId(table, 'table');
      const tablePath = `${CONFIG.paimon.warehouse}/${database}.db/${table}`;
      const schemaRaw = runWsl(`cat ${tablePath}/schema/schema-0 2>/dev/null || echo '{}'`);
      let schema = {}; try { schema = JSON.parse(schemaRaw); } catch (e) { schema = { raw: schemaRaw }; }
      const fields = schema.fields || [];
      const pk = schema.primaryKeys || [];
      schemaData = { columns: fields.map(f => ({ name:f.name, type:f.type, comment:f.comment||'', pk:pk.includes(f.name) })), primaryKey: pk };
    } else if (CATALOG_TYPES.jdbc.includes(cat.type)) {
      const conn = await getJdbcConnection(cat);
      try {
        const db = database || cat.connection?.database;
        const [rows] = await conn.query(
          `SELECT COLUMN_NAME as name, DATA_TYPE as type, IS_NULLABLE as nullable, COLUMN_KEY as colKey, COLUMN_COMMENT as comment
           FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
          [db, table]
        );
        const pk = rows.filter(r => r.colKey === 'PRI').map(r => r.name);
        schemaData = { columns: rows.map(r => ({ name:r.name, type:r.type, nullable:r.nullable==='YES', pk:r.colKey==='PRI', comment:r.comment||'' })), primaryKey: pk };
      } finally { conn.end(); }
    }

    if (!schemaData || !schemaData.success && !schemaData.columns) {
      return res.json({ success: false, error: schemaData?.error || '无法获取 Schema' });
    }

    // 根据源类型生成不同 DDL
    const { columns, primaryKey: pk } = schemaData;
    const typeMap = {
      int:'INT', integer:'INT', tinyint:'TINYINT', smallint:'SMALLINT', mediumint:'INT',
      bigint:'BIGINT', float:'FLOAT', double:'DOUBLE', decimal:'DECIMAL',
      char:'STRING', varchar:'STRING', text:'STRING', longtext:'STRING', mediumtext:'STRING',
      date:'DATE', datetime:'TIMESTAMP(3)', timestamp:'TIMESTAMP(3)', time:'TIME',
      json:'STRING', blob:'BYTES', binary:'BYTES'
    };

    let ddl = '';
    const colDefs = columns.map(r => {
      const flinkType = typeMap[String(r.type).toLowerCase()] || 'STRING';
      return `  \`${r.name}\` ${flinkType}${r.comment ? ` COMMENT '${r.comment}'` : ''}`;
    });
    const pkLine = pk.length > 0 ? `\n  PRIMARY KEY (${pk.map(c => `\`${c}\``).join(', ')}) NOT ENFORCED` : '';

    if (cat.type === 'mysql' || (cat.type === 'doris')) {
      // MySQL/Doris CDC DDL
      const c = cat.connection || {};
      ddl = `CREATE TABLE \`${table}\` (\n${colDefs.join(',\n')}${pkLine}\n) WITH (\n  'connector' = '${cat.type === 'doris' ? 'doris' : 'mysql-cdc'}',\n  'hostname' = '${c.host}',\n  'port' = '${c.port || 3306}',\n  'username' = '${c.user || 'root'}',\n  'password' = '${cat._password || '<YOUR_PASSWORD>'}',\n  'database-name' = '${database || c.database}',\n  'table-name' = '${table}'\n);`;
    } else if (cat.type === 'paimon') {
      // Paimon DDL（结果表）
      ddl = `CREATE TABLE \`${table}\` (\n${colDefs.join(',\n')}${pkLine}\n) WITH (\n  'connector' = 'paimon',\n  'warehouse' = '${cat.props?.warehouse || CONFIG.paimon.warehouse}',\n  'database' = '${database}',\n  'table' = '${table}'\n);`;
    } else {
      // 通用 DDL
      ddl = `CREATE TABLE \`${table}\` (\n${colDefs.join(',\n')}${pkLine}\n);`;
    }

    // 密码脱敏
    const maskedDdl = cat._password
      ? ddl.replace(new RegExp(`'password' = '${cat._password.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`, 'g'), `'password' = '******'`)
      : ddl;

    res.json({ success: true, ddl: maskedDdl, tableName: table, columns: columns.length, primaryKey: pk });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 初始化 Doris Catalog（在服务启动时自动执行）
const initDorisCatalogs = async () => {
  const all = getCatalogs();
  let conn;
  try {
    conn = await getDorisConn();
    for (const cat of Object.values(all)) {
      if ((cat.engine === 'doris' || cat.engine === 'both') && cat.type === 'paimon') {
        try {
          const dorisProps = cat.dorisProps || cat.props || {};
          const warehouse = dorisProps.warehouse || `file://${dorisProps.warehouse_path || '/mnt/g/paimon_warehouse'}`;
          const paimonCatType = dorisProps.paimon_catalog_type || 'filesystem';
          if (!validateIdentifier(cat.id, 'catalog id')) { console.warn(`⚠️ 跳过无效 Catalog ID: ${cat.id}`); continue; }
          await conn.query(`CREATE CATALOG IF NOT EXISTS ${safeId(cat.id)} PROPERTIES ('type'='paimon','paimon.catalog.type'='${paimonCatType}','warehouse'='${warehouse}')`);
          console.log(`✅ Doris Catalog "${cat.id}" 已初始化`);
        } catch (e) {
          console.warn(`⚠️ Doris Catalog "${cat.id}" 初始化失败: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.warn('⚠️ Doris 未就绪，Catalog 初始化跳过');
  } finally {
    if (conn) conn.end();
  }
};

// ==================== Flink API ====================
const FLINK_TIMEOUT = 8000; // 8s 超时（Flink/Doris 未启动时不卡死前端）
const fetchFlink = async (path, timeoutMs = FLINK_TIMEOUT) => {
  const { default: fetch } = await import('node-fetch');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${CONFIG.flink.baseUrl}${path}`, { signal: controller.signal });
    if (!res.ok) throw new Error(`Flink API error: ${res.status}`);
    return res.json();
  } finally {
    clearTimeout(timer);
  }
};

// Flink: 作业列表
// Flink: 作业列表 (包含历史作业)
app.get('/api/flink/jobs', async (req, res) => {
  try {
    // 1. 从 Flink API 获取当前活跃作业
    const flinkApiJobsRaw = await fetchFlink('/jobs/overview');
    const flinkApiJobs = (flinkApiJobsRaw.jobs || []).map(j => ({
      id: j.jid,
      name: j.name,
      state: j.state,
      startTime: j['start-time'],
      endTime: j['end-time'],
      duration: j.duration,
      type: 'streaming', // Flink overview 接口通常只返回流式作业
      savepointPath: getSavepoint(j.name)?.path || null, // 从 savepoint store 获取
    }));

    // 2. 从本地历史存储获取所有作业（包括历史和已提交但未启动的）
    const historyJobs = getJobHistory();

    // 2.5 获取已删除黑名单，过滤掉用户主动删除的作业
    // Debug log helper (定义在使用前)
    const deletedSet = new Set(getDeletedJobs());

    // ★ 2.8 收集所有 batch 记录的 flinkJobIds（这些 ID 已被聚合记录吸收，不应单独出现）
    const absorbedIds = new Set();
    for (const jid in historyJobs) {
      const hj = historyJobs[jid];
      if (hj?._isBatch && Array.isArray(hj.flinkJobIds)) {
        for (const fid of hj.flinkJobIds) absorbedIds.add(fid);
      }
    }

    // 3. 合并和去重
    const allJobsMap = new Map(); // Map<jobId, jobData>

    // 先添加历史作业（作为基础，包含更多元数据）
    for (const jobId in historyJobs) {
      if (historyJobs.hasOwnProperty(jobId) && !deletedSet.has(jobId)) {
        allJobsMap.set(jobId, historyJobs[jobId]);
      }
    }

    // 再添加 Flink API 活跃作业，并覆盖历史记录中的同 ID 作业状态
    for (const flinkJob of flinkApiJobs) {
      // 跳过已删除的作业（按 ID）
      if (deletedSet.has(flinkJob.id)) continue;
      // ★ BUG #153: 跳过已删除的作业（按 jobName）——防止 _detectionTimeout 等场景下 Flink ID 未被黑名单
      if (flinkJob.name && deletedSet.has(flinkJob.name)) continue;
      // ★ 跳过已被 batch 聚合记录吸收的 Flink Job ID（避免重复出现）
      if (absorbedIds.has(flinkJob.id)) continue;
      // 如果历史中有该作业，则更新其状态、持续时间等实时信息
      if (allJobsMap.has(flinkJob.id)) {
        const existingJob = allJobsMap.get(flinkJob.id);
        // 关键修复：如果作业处于 RESUMING 状态，不要用 Flink 的旧状态覆盖！
        // 因为 RESUMING 表示正在等待新 Flink 作业启动，Flink 中的同 ID 作业
        // 已是 CANCELED（暂停后的旧状态），覆盖会导致恢复流程中断
        if (existingJob.state === 'RESUMING') {
          // 保持 RESUMING 状态不变，跳过 Flink 状态覆盖
        } else {
          // 如果 Flink 确认作业在运行，且 history 中有 resume 残留字段，说明恢复已完成，清理残留
          const cleaned = { ...existingJob };
          if (flinkJob.state === 'RUNNING' && (cleaned.resumeTargetName || cleaned.resumeTime)) {
            delete cleaned.resumeTargetName;
            delete cleaned.resumeTime;
            delete cleaned.resumedFrom;
            // 异步持久化清理
            const _h2 = getJobHistory();
            if (_h2[flinkJob.id]) {
              delete _h2[flinkJob.id].resumeTargetName;
              delete _h2[flinkJob.id].resumeTime;
              delete _h2[flinkJob.id].resumedFrom;
              saveJobHistory(_h2);
            }
          }
          allJobsMap.set(flinkJob.id, {
            ...cleaned,
            state: flinkJob.state,
            startTime: flinkJob.startTime,
            endTime: flinkJob.endTime,
            duration: flinkJob.duration,
          });
        }
      } else {
        // 如果历史中没有该作业 ID，则添加为新作业
        // ★ BUG #89 修复：检测是否有同名 RESUMING 记录正在等待合并
        // Resume 流程：旧记录标记为 RESUMING → Flink 从 savepoint 启动新 Job（新 ID）
        // → Step 3.5 通过名称匹配做 key 迁移。如果这里直接添加新 ID，就会产生重复！
        const hasResumingSameName = [...allJobsMap.values()].some(
          existing => (existing.state === 'RESUMING' || existing.resumeTargetName) &&
                     existing.name === flinkJob.name
        );
        if (hasResumingSameName) {
          // 有同名 RESUMING 记录 → 跳过，交给 Step 3.5 做名称匹配合并
          // 不在这里创建新记录，避免出现 2 条 RUNNING
        } else {
          allJobsMap.set(flinkJob.id, flinkJob);
        }
      }
    }

    // 3.5 Resume 合并（名称匹配策略）
    const resumingEntries = [];
    for (const [jid, job] of allJobsMap) {
      if (job.state === 'RESUMING' || (job.resumeTargetName && job.state !== 'CANCELED' && job.state !== 'FINISHED' && job.state !== 'FAILED')) {
        resumingEntries.push({ jid, job });
      }
    }
    for (const { jid, job } of resumingEntries) {
      const targetName = job.resumeTargetName || job.name;
      // 在 Flink API 返回的作业中查找同名 RUNNING 作业（排除自己和已删除）
      const flinkMatch = flinkApiJobs.find(f =>
        f.name === targetName &&
        f.state === 'RUNNING' &&
        f.id !== jid &&
        !deletedSet.has(f.id) &&
        !deletedSet.has(f.name) // ★ BUG #153: name 也查黑名单
      );
      if (flinkMatch) {
        // 找到了！用 Flink 真实 ID 重建记录，继承 history 元数据
        const { resumeTargetName, resumeTime, resumedFrom, ...coreJob } = job;
        const merged = {
          ...coreJob,
          id: flinkMatch.id,            // 迁移到 Flink 真实 ID
          state: flinkMatch.state,       // RUNNING
          startTime: flinkMatch.startTime,
          endTime: flinkMatch.endTime,
          duration: flinkMatch.duration,
          state: 'RUNNING',              // 明确标记为已恢复运行
        };
        // 删除旧 key
        allJobsMap.delete(jid);
        // 也删除 Flink 原始条目（避免重复）
        if (allJobsMap.has(flinkMatch.id)) {
          allJobsMap.delete(flinkMatch.id);
        }
        // 用真实 ID 写入
        allJobsMap.set(flinkMatch.id, merged);
        // 同步持久化到 history 文件（key 迁移到新 ID）
        const history = getJobHistory();
        if (history[jid]) {
          delete history[jid];
          history[flinkMatch.id] = merged;
          saveJobHistory(history);
        }
      } else {
        // Flink 还没有同名作业（可能还在启动中），保持 RESUMING 状态显示
        if (job.state === 'RESUMING') {
          // RESUMING 超过 5 分钟则回退为 CANCELED（说明恢复失败）
          const resumeAge = Date.now() - new Date(job.resumeTime || job.startTime || 0).getTime();
          if (resumeAge > 5 * 60 * 1000) {
            allJobsMap.set(jid, { ...job, state: 'CANCELED' });
            const history = getJobHistory();
            if (history[jid]) {
              history[jid].state = 'CANCELED';
              saveJobHistory(history);
            }
          }
        }
      }
    }

    // ★ 3.5 Batch 聚合状态更新（Task #122）
    // 对于 _isBatch 记录，查询其所有 flinkJobIds 的实时状态，聚合为整体状态
    for (const [jid, job] of allJobsMap) {
      if (!job._isBatch || !Array.isArray(job.flinkJobIds) || job.flinkJobIds.length === 0) continue;

      const subStates = [];
      let earliestStart = job.startTime;
      let latestEnd = null;

      for (const fid of job.flinkJobIds) {
        const flinkJob = flinkApiJobs.find(f => String(f.id) === String(fid));
        if (flinkJob) {
          subStates.push(flinkJob.state || 'UNKNOWN');
          // 取最早开始时间
          const fs = flinkJob['start-time'] || flinkJob.startTime;
          if (fs && (!earliestStart || new Date(fs).getTime() < new Date(earliestStart).getTime())) {
            earliestStart = new Date(fs).toISOString();
          }
          // 取最晚结束时间
          const fe = flinkJob['end-time'] || flinkJob.endTime;
          if (fe) {
            const feIso = new Date(fe).toISOString();
            if (!latestEnd || new Date(feIso).getTime() > new Date(latestEnd).getTime()) {
              latestEnd = feIso;
            }
          }
        } else {
          subStates.push('UNKNOWN');
        }
      }

      // 聚合状态判定：任一 RUNNING → RUNNING；任一 FAILED → FAILED；全部 FINISHED → FINISHED；其余 CANCELED
      let aggregatedState = job.state; // 默认保留原状态
      if (subStates.length > 0) {
        if (subStates.some(s => s === 'RUNNING')) {
          aggregatedState = 'RUNNING';
        } else if (subStates.some(s => s === 'FAILED')) {
          aggregatedState = 'FAILED';
        } else if (subStates.every(s => s === 'FINISHED')) {
          aggregatedState = 'FINISHED';
        } else if (subStates.every(s => s === 'CANCELED' || s === 'CANCELLING')) {
          aggregatedState = 'CANCELED';
        } else if (!subStates.includes('INITIALIZING') && !subStates.includes('CREATED') && !subStates.includes('RECONCILING')) {
          aggregatedState = 'CANCELED'; // 所有子 Job 都已终态但状态混杂
        }
        // 如果还有 INITIALIZING/CREATED 等中间态，保持原状态不变继续等待
      }

      // 更新 batch 记录的状态和时间字段
      allJobsMap.set(jid, {
        ...job,
        state: aggregatedState,
        startTime: earliestStart || job.startTime,
        endTime: latestEnd || job.endTime,
      });

      // 同步持久化到 history 文件
      try {
        const history = getJobHistory();
        if (history[jid]) {
          history[jid].state = aggregatedState;
          if (earliestStart) history[jid].startTime = earliestStart;
          if (latestEnd) history[jid].endTime = latestEnd;
          saveJobHistory(history);
        }
      } catch (e) {
        logError('batch-aggregate-persist', e.message);
      }
    }

    // 3.6 全局同名去重（BUG #89 根治）
    // 问题：多次 Pause→Resume 会在 history 中积累多份同名记录（CANCELED×N + RESUMING×1）
    // 方案：同一个 jobName 只保留最相关的 1 条记录，优先级：RUNNING > RESUMING > CANCELED > 其他
    const nameAllGroups = {};
    for (const [jid, job] of allJobsMap) {
      const n = job.name || '_unnamed';
      if (!nameAllGroups[n]) nameAllGroups[n] = [];
      nameAllGroups[n].push({ jid, job });
    }
    for (const [name, entries] of Object.entries(nameAllGroups)) {
      if (entries.length <= 1) continue; // 无重复，跳过
      // 去重优先级：RUNNING > RESUMING > 有 savepoint 的 CANCELED > 其它
      const statePriority = { 'RUNNING': 0, 'RESUMING': 1, 'CANCELED': 2, 'FINISHED': 3, 'FAILED': 4, 'UNKNOWN': 5 };
      // 先按优先级排序，同优先级时按 startTime 降序（最新的在前面）
      entries.sort((a, b) => {
        const pa = statePriority[a.job.state] ?? 9;
        const pb = statePriority[b.job.state] ?? 9;
        if (pa !== pb) return pa - pb;
        const ta = new Date(a.job.startTime || 0).getTime();
        const tb = new Date(b.job.startTime || 0).getTime();
        return tb - ta;
      });
      // 保留第一条（最高优先级），其余删除
      const keep = entries[0];
      for (let i = 1; i < entries.length; i++) {
        allJobsMap.delete(entries[i].jid);
      }
      // 同步清理 history 文件中的重复记录（持久化去重）
      try {
        const history = getJobHistory();
        let changed = false;
        for (let i = 1; i < entries.length; i++) {
          if (history[entries[i].jid]) {
            delete history[entries[i].jid];
            changed = true;
          }
        }
        if (changed) saveJobHistory(history);
      } catch (e) { /* history 清理失败不阻塞响应 */ }
    }

    // 3.7 血缘补全（Task #128）：对检测超时的记录，匹配到 Flink Job 后回填 lineage
    for (const [jid, job] of allJobsMap) {
      if (!job._detectionTimeout || job.primaryJobId) continue;
      // 通过 jobName 在 Flink API 返回的作业中查找
      const flinkMatch = flinkApiJobs.find(f => f.name === job.name && f.jid);
      if (flinkMatch) {
        const realJobId = flinkMatch.jid;
        // 回填 history
        job.primaryJobId = realJobId;
        job.flinkJobIds = [realJobId];
        try {
          const _h = getJobHistory();
          if (_h[jid]) {
            _h[jid].primaryJobId = realJobId;
            _h[jid].flinkJobIds = [realJobId];
            delete _h[jid]._detectionTimeout;
            saveJobHistory(_h);
          }
        } catch (e) { /* ignore */ }
        // 回填 lineage
        try {
          const lineageStore = getLineageStore();
          const lineageRec = lineageStore.records.find(r => r.batchId === jid);
          if (lineageRec && !lineageRec.jobId) {
            lineageRec.jobId = realJobId;
            lineageRec.flinkJobIds = [realJobId];
            lineageRec.primaryJobId = realJobId;
            delete lineageRec._detectionTimeout;
            saveLineageStore(lineageStore);
          }
        } catch (e) { logError('lineage-backfill', e.message); }
      }
    }

    // 4. 对作业进行排序（例如按开始时间倒序）
    const sortedJobs = Array.from(allJobsMap.values()).sort((a, b) => {
      const timeA = new Date(a.startTime || 0).getTime();
      const timeB = new Date(b.startTime || 0).getTime();
      return timeB - timeA; // 最新提交的在前面
    });

    res.json({ success: true, data: sortedJobs });
  } catch (e) {
    console.error('Error fetching Flink jobs:', e);
    res.json({ success: false, error: e.message, data: [] });
  }
});

// Flink: 集群概览
app.get('/api/flink/overview', async (req, res) => {
  try {
    const data = await fetchFlink('/overview');
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message, data: {} });
  }
});

// Flink: 作业详情（合并 history 中的 SQL 等元数据）
app.get('/api/flink/jobs/:jobId', async (req, res) => {
  try {
    const data = await fetchFlink(`/jobs/${req.params.jobId}`);
    const jobData = data || {};
    // 合并 history 中的元数据（SQL、flinkJobIds 等）
    const history = getJobHistory();
    const histRecord = history[req.params.jobId] || null;
    if (histRecord) {
      // 附加 history 中的字段（不覆盖 Flink 原生字段）
      if (histRecord.sql && !jobData.sql) jobData.sql = histRecord.sql;
      if (histRecord.flinkJobIds && !jobData.flinkJobIds) jobData.flinkJobIds = histRecord.flinkJobIds;
      if (histRecord.primaryJobId && !jobData.primaryJobId) jobData.primaryJobId = histRecord.primaryJobId;
      // ★ 将 Flink 原生 JobID 映射到 jid，方便前端拉取 exceptions/checkpoints
      if (histRecord.primaryJobId && !jobData.jid) jobData.jid = histRecord.primaryJobId;
      if (histRecord.type && !jobData.type) jobData.type = histRecord.type;
      if (histRecord._isBatch) jobData._isBatch = histRecord._isBatch;
    }
    res.json({ success: true, data: jobData });
  } catch (e) {
    // 即使 Flink API 失败，也尝试返回 history 数据
    try {
      const history = getJobHistory();
      const histRecord = history[req.params.jobId] || null;
      if (histRecord) {
        const fallback = { ...histRecord, _historyOnly: true };
        // ★ 将 Flink 原生 JobID 映射到 jid 字段，方便前端用 flinkJobId 拉取 exceptions/checkpoints
        if (histRecord.primaryJobId && !fallback.jid) fallback.jid = histRecord.primaryJobId;
        return res.json({ success: true, data: fallback });
      }
    } catch (e2) { /* ignore */ }
    res.json({ success: false, error: e.message });
  }
});

// Flink: 作业 Plan（DAG 数据）
app.get('/api/flink/jobs/:jobId/plan', async (req, res) => {
  try {
    const data = await fetchFlink(`/jobs/${req.params.jobId}/plan`);
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 顶点背压状态
app.get('/api/flink/jobs/:jobId/vertices/:vertexId/backpressure', async (req, res) => {
  try {
    const data = await fetchFlink(`/jobs/${req.params.jobId}/vertices/${req.params.vertexId}/backpressure`);
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 读取 Flink 提交 SQL 文件（兜底）
app.get('/api/flink/last-sql', (req, res) => {
  try {
    // 1. 先读 store
    let sql = getSqlFromStore() || '';
    // 2. fallback 到 WSL 内已知 SQL 文件
    if (!sql) {
      try {
        sql = execSync(`wsl.exe -e bash -c "cat ${CONFIG.flink.home}/flinkcdc_stream.sql 2>/dev/null || cat ${CONFIG.flink.home}/*.sql 2>/dev/null || echo -n""`).toString();
      } catch (e) {}
    }
    res.json({ success: true, sql });
  } catch (e) {
    res.json({ success: false, sql: '', error: e.message });
  }
});

// Flink: 作业异常
app.get('/api/flink/jobs/:jobId/exceptions', async (req, res) => {
  try {
    const { flinkJobId } = resolveFlinkJobId(req.params.jobId);
    const url = flinkJobId ? `/jobs/${flinkJobId}/exceptions` : `/jobs/${req.params.jobId}/exceptions`;
    const data = await fetchFlink(url);
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: Checkpoint 状态
app.get('/api/flink/jobs/:jobId/checkpoints', async (req, res) => {
  try {
    const { flinkJobId } = resolveFlinkJobId(req.params.jobId);
    const url = flinkJobId ? `/jobs/${flinkJobId}/checkpoints` : `/jobs/${req.params.jobId}/checkpoints`;
    const data = await fetchFlink(url);
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// 工具函数：根据前端传入的 jid（可能是 history key 或真实 Flink Job ID）解析真实 Flink Job ID
// 返回 { flinkJobId, historyRecord, historyKey }
function resolveFlinkJobId(jid) {
  // 1. 先尝试在 history 中直接匹配 key
  const history = getJobHistory();
  if (history[jid]) {
    const rec = history[jid];
    // 优先用 primaryJobId，其次 flinkJobIds[0]
    const fid = rec.primaryJobId || (rec.flinkJobIds && rec.flinkJobIds[0]) || null;
    if (fid) return { flinkJobId: fid, historyRecord: rec, historyKey: jid };
    // history 中有记录但没有 flinkJobId（如 _detectionTimeout 记录），无法操作
    return { flinkJobId: null, historyRecord: rec, historyKey: jid };
  }
  // 2. 尝试按 jobName 匹配
  const byName = Object.entries(history).find(([k, v]) => v.name === jid);
  if (byName) {
    const [key, rec] = byName;
    const fid = rec.primaryJobId || (rec.flinkJobIds && rec.flinkJobIds[0]) || null;
    if (fid) return { flinkJobId: fid, historyRecord: rec, historyKey: key };
    return { flinkJobId: null, historyRecord: rec, historyKey: key };
  }
  // 3. 直接用 jid 作为 Flink Job ID（兼容旧逻辑/直接传 Flink ID 的场景）
  return { flinkJobId: jid, historyRecord: null, historyKey: null };
}

// Flink: 取消作业
app.patch('/api/flink/jobs/:jobId/cancel', async (req, res) => {
  try {
    const { default: fetch } = await import('node-fetch');
    const { flinkJobId, historyKey } = resolveFlinkJobId(req.params.jobId);
    if (!flinkJobId) {
      return res.json({ success: false, error: '无法解析 Flink Job ID，该作业可能尚未成功提交或已被清理' });
    }
    const r = await fetch(`${CONFIG.flink.baseUrl}/jobs/${flinkJobId}?mode=cancel`, { method: 'PATCH' });
    // 更新 history 状态
    if (historyKey) {
      try {
        const history = getJobHistory();
        if (history[historyKey]) {
          history[historyKey].state = 'CANCELED';
          history[historyKey].endTime = new Date().toISOString();
          saveJobHistory(history);
        }
      } catch (e) { /* ignore */ }
    }
    res.json({ success: true });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Savepoint 持久化
const SAVEPOINT_STORE = path.join(CONFIG.dataDir, 'lakehouse_savepoints.json');
const loadSavepoints = () => {
  try { return JSON.parse(fs.readFileSync(SAVEPOINT_STORE, 'utf8')); }
  catch (e) { return {}; }
};
const saveSavepoint = (jobName, path) => {
  const store = loadSavepoints();
  store[jobName] = { path, time: Date.now() };
  fs.writeFileSync(SAVEPOINT_STORE, JSON.stringify(store, null, 2));
};
const getSavepoint = (jobName) => {
  const store = loadSavepoints();
  return store[jobName] || null;
};

// Flink: 暂停作业（savepoint + cancel）
app.patch('/api/flink/jobs/:jobId/pause', async (req, res) => {
  try {
    const { default: fetch } = await import('node-fetch');
    const { flinkJobId, historyRecord, historyKey } = resolveFlinkJobId(req.params.jobId);
    if (!flinkJobId) {
      return res.json({ success: false, error: '无法解析 Flink Job ID，该作业可能尚未成功提交或已被清理' });
    }

    // 1. 获取作业名称
    let jobName = historyRecord?.name || '';
    if (!jobName) {
      try {
        const jobInfo = await (await fetch(`${CONFIG.flink.baseUrl}/jobs/${flinkJobId}`)).json();
        jobName = jobInfo.name || '';
      } catch (e) {}
    }

    // 2. 触发 savepoint 并 cancel
    const savepointDir = CONFIG.flink.savepointDir;
    const spRes = await fetch(`${CONFIG.flink.baseUrl}/jobs/${flinkJobId}/savepoints`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 'cancel-job': true, 'target-directory': savepointDir })
    });
    if (!spRes.ok) {
      const text = await spRes.text();
      return res.json({ success: false, error: `触发 savepoint 失败: ${text}` });
    }
    const spData = await spRes.json();
    const requestId = spData['request-id'];
    if (!requestId) {
      return res.json({ success: false, error: 'Flink 未返回 request-id' });
    }

    // 3. 轮询 savepoint 完成状态（最多 30 秒）
    let savepointPath = '';
    for (let i = 0; i < 30; i++) {
      await new Promise(r => setTimeout(r, 1000));
      const poll = await fetch(`${CONFIG.flink.baseUrl}/jobs/${flinkJobId}/savepoints/${requestId}`);
      if (!poll.ok) continue;
      const pollData = await poll.json();
      if (pollData.status?.id === 'COMPLETED') {
        savepointPath = pollData.operation?.location || '';
        break;
      }
      if (pollData.status?.id === 'FAILED') {
        return res.json({ success: false, error: `Savepoint 失败: ${pollData.status?.description || '未知错误'}` });
      }
    }

    if (!savepointPath) {
      return res.json({ success: false, error: 'Savepoint 生成超时（30s），请在 Flink Web UI 查看状态' });
    }

    // 4. 持久化 savepoint 路径并更新作业历史
    if (jobName && historyKey) {
      saveSavepoint(jobName, savepointPath);
      const history = getJobHistory();
      if (history[historyKey]) {
        history[historyKey].state = 'CANCELED'; // Flink 暂停操作实际上是 cancel
        history[historyKey].endTime = new Date().toISOString();
        history[historyKey].savepointPath = savepointPath;
        saveJobHistory(history);
      }
    }

    res.json({ success: true, savepointPath, jobName });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 恢复作业（从 savepoint 重新提交）
// 支持两种调用方式：
//   1. PATCH /api/flink/jobs/:jobId/resume  {jobName, savepointPath?}
//   2. PATCH /api/flink/jobs/any/resume     {jobName, savepointPath?}  ← 兼容旧前端
app.patch('/api/flink/jobs/:jobId/resume', async (req, res) => {
  try {
    const { jobId } = req.params;
    const { jobName: bodyJobName, savepointPath: bodySp } = req.body;
    // 如果 jobId === 'any'，则从 body 中拿 jobName 去 history 匹配真实 ID
    let realJobId = jobId;
    let resolvedJobName = bodyJobName || '';
    if (jobId === 'any') {
      // 兼容旧前端：用 jobName 查找 history 中的真实 jobId
      if (!resolvedJobName) {
        return res.json({ success: false, error: '使用 /any/resume 时必须提供 jobName' });
      }
      const history = getJobHistory();
      const found = Object.entries(history).find(([k, v]) => v.name === resolvedJobName);
      if (!found) {
        return res.json({ success: false, error: `未找到名为 "${resolvedJobName}" 的作业历史` });
      }
      realJobId = found[0]; // 用 history key 作为真实 jobId
    }
    const jobName = req.body.jobName || '';
    if (!jobName) {
      return res.json({ success: false, error: '缺少 jobName 参数' });
    }

    const sp = getSavepoint(jobName);
    if (!sp || !sp.path) {
      return res.json({ success: false, error: `未找到作业 "${jobName}" 的 savepoint，请先暂停` });
    }

    // 获取该作业名对应的 SQL
    const sql = getSqlFromStore(jobName);
    if (!sql) {
      return res.json({ success: false, error: 'SQL Store 为空，无法自动恢复。请手动在数据开发中提交 SQL。' });
    }

    // 构建带 savepoint 的 SQL（如果已存在则不重复追加）
    let savepointSql = sql;
    if (!sql.includes("execution.savepoint.path")) {
      savepointSql = `SET 'execution.savepoint.path' = '${sp.path}';
\n${sql}`;
    }

    const ts = Date.now();
    const tmpFile = `/tmp/lakehouse_submit_${ts}.sql`;
    const logFile = `/tmp/lakehouse_submit_${ts}.log`;
    saveSqlToStore(sql, jobName);

    const catalogs = loadCatalogs();
    const flinkCats = Object.values(catalogs).filter(c => c.engine === 'flink' || c.engine === 'both');
    const catalogStmts = flinkCats.map(c => {
      if (c.type === 'paimon') {
        const w = c.props.warehouse || '/mnt/g/paimon_warehouse';
        const wPath = w.startsWith('/') ? w : '/' + w;
        return `CREATE CATALOG IF NOT EXISTS ${c.id} WITH (\n  'type' = 'paimon',\n  'paimon.catalog.type' = '${c.props.paimon_catalog_type || 'filesystem'}',\n  'warehouse' = 'file://${wPath}'\n);`;
      }
      return '';
    }).filter(Boolean).join('\n');

    const fullSql = catalogStmts ? `${catalogStmts}\n\n${savepointSql}` : savepointSql;
    const b64 = Buffer.from(fullSql).toString('base64');
    execSync(`wsl.exe -e bash -c "echo '${b64}' | base64 -d > ${tmpFile} && chmod 644 ${tmpFile}"`);

    // 异步执行 sql-client.sh（不阻塞响应）
    const child = exec(
      `wsl.exe -e bash -c "cd ${CONFIG.flink.home} && timeout 120 bash bin/sql-client.sh -f ${tmpFile} > ${logFile} 2>&1; echo '==RESUME_DONE==' >> ${logFile}"`,
      { timeout: 0 },
      (err) => { if (err) console.error('Resume exec error:', err.message); }
    );
    child.unref();

    // === 根本性修复：不猜测 newJobId，改为标记原记录为"恢复中" ===
    // GET /api/flink/jobs 会通过名称匹配将 Flink 新作业合并到该记录
    const history = getJobHistory();
    if (history[realJobId]) {
      history[realJobId] = {
        ...history[realJobId],
        state: 'RESUMING',
        startTime: new Date().toISOString(),
        endTime: null,
        duration: null,
        logFile: logFile,
        resumedFrom: realJobId,
        resumeTargetName: jobName,
        resumeTime: new Date().toISOString(),
      };
      delete history[realJobId].savepointPath;
      saveJobHistory(history);
    }

    // 立即返回，让前端轮询刷新
    res.json({
      success: true,
      message: '✅ 作业已从 savepoint 提交恢复，正在启动中...',
      jobId: realJobId,
      savepointPath: sp.path,
      tmpFile,
      logFile,
      mode: 'resume-async'
    });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 删除作业历史记录
app.delete('/api/flink/jobs/:jobId/history', async (req, res) => {
  try {
    const jobId = req.params.jobId;
    const history = getJobHistory();
    // 从历史记录中删除
    let deleted = false;
    const historyRecord = history[jobId] || null;
    if (history[jobId]) {
      delete history[jobId];
      saveJobHistory(history);
      deleted = true;
    }
    // 加入已删除黑名单（防止 Flink API 再次返回该作业时重新出现）
    // ★ BUG #147 修复：同时将该作业关联的 Flink Job ID 也加入黑名单
    // 根因：前端 j.id 是 history key（如 batch_xxx 或 32位hex），
    //       而 Flink API 返回的是 flinkJobId（长数字/短 hex）
    // 如果只黑名单 history key，Flink 返回的作业用 flinkJobId 匹配不上 → 重新出现
    // ★ BUG #153 修复：增加 jobName 黑名单 + 删除时主动 cancel Flink 作业
    const deletedJobs = getDeletedJobs();
    const idsToBlacklist = [jobId];
    if (historyRecord) {
      // 提取所有关联的 Flink Job ID（支持单 Job 和 Batch 多 Job 场景）
      if (historyRecord.primaryJobId) idsToBlacklist.push(String(historyRecord.primaryJobId));
      if (historyRecord.flinkJobIds && Array.isArray(historyRecord.flinkJobIds)) {
        for (const fid of historyRecord.flinkJobIds) {
          if (fid) idsToBlacklist.push(String(fid));
        }
      }
      // ★ BUG #153: 同时把 jobName 加入黑名单（用于 name-based 过滤）
      if (historyRecord.name && !idsToBlacklist.includes(historyRecord.name)) {
        idsToBlacklist.push(historyRecord.name);
      }
      // ★ BUG #153: 如果作业可能还在 Flink 上运行，主动尝试 cancel
      // resolveFlinkJobId 能找到真实 Flink ID 就 cancel，找不到就跳过（无副作用）
      try {
        const resolved = resolveFlinkJobId(jobId);
        if (resolved.flinkJobId) {
          await fetchFlink(`/jobs/${resolved.flinkJobId}`, { method: 'PATCH' });
          logInfo('job-delete-cancel', `删除历史时同步取消 Flink 作业: ${resolved.flinkJobId} (${historyRecord.name || jobId})`);
        }
      } catch (cancelErr) {
        // cancel 失败不阻塞删除（作业可能已结束）
        logInfo('job-delete-cancel-skip', `取消 Flink 作业失败(可忽略): ${cancelErr.message}`);
      }
    }
    let blacklistChanged = false;
    for (const id of idsToBlacklist) {
      if (!deletedJobs.includes(id)) {
        deletedJobs.push(id);
        blacklistChanged = true;
      }
    }
    if (blacklistChanged) {
      saveDeletedJobs(deletedJobs);
      deletedJobsCache = deletedJobs; // 更新缓存
      logInfo('job-delete-blacklist', `黑名单新增: [${idsToBlacklist.join(', ')}], 总计: ${deletedJobs.length}`);
    }
    // ★ Task #124: 级联删除关联的血缘记录
    try {
      const lineageDeleted = lineageDeleteByJobId(jobId);
      if (lineageDeleted > 0) {
        logInfo('lineage-cascade-delete', `已级联删除 ${lineageDeleted} 条血缘记录（jobId=${jobId}）`);
      }
    } catch (e) { logError('lineage-cascade-delete', e.message); }
    res.json({ success: true, message: `作业 ${jobId} 历史记录已删除` });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 获取已存储的 savepoints
app.get('/api/flink/savepoints', (req, res) => {
  try {
    const store = loadSavepoints();
    res.json({ success: true, data: store });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 资源配置（TM/JM CPU/内存/Slots）
app.get('/api/flink/resources', (req, res) => {
  try {
    const saved = loadFlinkResources();
    const actual = parseFlinkConfig();
    res.json({ success: true, data: { saved, actual } });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});
app.post('/api/flink/resources', (req, res) => {
  try {
    const { jobManager, taskManager, parallelism, restartStrategy } = req.body;
    // 基本校验
    if (!jobManager || !taskManager) return res.json({ success: false, error: '缺少 jobManager 或 taskManager 配置' });
    const jmMem = String(jobManager.memory || '').trim();
    const tmMem = String(taskManager.memory || '').trim();
    const tmSlots = parseInt(taskManager.slots) || 1;
    const par = parseInt(parallelism) || 1;
    // 支持 "1600mb", "2gb", "2g", "1024" (默认mb), "1.5gb" 等格式
    if (!/^\d+(?:\.\d+)?[mggbkMGGBK]?b?$/i.test(jmMem)) return res.json({ success: false, error: 'JM 内存格式错误，如 1600mb、2gb' });
    if (!/^\d+(?:\.\d+)?[mggbkMGGBK]?b?$/i.test(tmMem)) return res.json({ success: false, error: 'TM 内存格式错误，如 2048mb、4gb' });
    const data = {
      jobManager: { memory: jmMem.toLowerCase(), cpu: Math.max(1, parseInt(jobManager.cpu) || 1) },
      taskManager: { memory: tmMem.toLowerCase(), cpu: Math.max(1, parseInt(taskManager.cpu) || 1), slots: Math.max(1, tmSlots) },
      parallelism: Math.max(1, par),
    };
    // 保存重启策略（可选）
    if (restartStrategy && restartStrategy.strategy) {
      const validStrategies = ['fixed-delay', 'failure-rate', 'none'];
      if (!validStrategies.includes(restartStrategy.strategy)) return res.json({ success: false, error: '无效的重启策略: ' + restartStrategy.strategy });
      data.restartStrategy = {
        strategy: restartStrategy.strategy,
        attempts: Math.max(1, parseInt(restartStrategy.attempts) || 3),
        delayVal: Math.max(1, parseInt(restartStrategy.delayVal) || 10),
        delayUnit: String(restartStrategy.delayUnit || 's'),
      };
    }
    saveFlinkResources(data);
    res.json({ success: true, message: '资源配置已保存', data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 执行查询（SELECT/SHOW/DESCRIBE/EXPLAIN）并返回结果
app.post('/api/flink/execute', async (req, res) => {
  const { sql, mode, cp, parallelism, restartStrategy, restartAttempts, restartDelayVal, restartDelayUnit, catalog: reqCatalog,
    // ★ 资源参数
    jmMemory, jmCpu, tmMemory, tmCpu, tmSlots
  } = req.body;
  if (!sql) return res.json({ success: false, error: 'SQL 不能为空' });
  // ★ BUG #144 debug: 记录接收到的并行度参数
  logInfo('flink-execute-params', `parallelism=${parallelism}, mode=${mode}, cp=${cp}`);
  try {
    const ts = Date.now();
    const tmpFile = `/tmp/lakehouse_exec_${ts}.sql`;
    const outFile = `/tmp/lakehouse_exec_${ts}.out`;

    // 前置 Catalog 配置：只为 Paimon 类型 catalog 生成 CREATE CATALOG 语句
    const catalogs = getCatalogs();
    const effectiveCatalog = reqCatalog || null;
    const catalogStmts = Object.values(catalogs)
      .filter(c => c.type === 'paimon' && (effectiveCatalog ? c.id === effectiveCatalog : true))
      .map(c => {
        const w = c.props.warehouse || '/mnt/g/paimon_warehouse';
        const wPath = w.startsWith('/') ? w : '/' + w;
        return `CREATE CATALOG IF NOT EXISTS ${c.id} WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = '${c.props.paimon_catalog_type || 'filesystem'}',
  'warehouse' = 'file://${wPath}'
);`;
      }).join('\n');

    // 查询 SQL：TABLEAU 模式 + 用户指定的执行模式（默认 BATCH）+ 动态参数
    const execMode = (mode || 'batch').toUpperCase();

    // ★ 动态构建 SET 参数——一切以用户输入为准
    const dynamicSets = [];
    // 1. 执行模式
    dynamicSets.push(`SET 'execution.runtime-mode' = '${execMode}';`);
    // 2. Checkpoint interval（用户设置才注入）
    if (cp) {
      dynamicSets.push(`SET 'execution.checkpointing.interval' = '${cp}';`);
    }
    // 3. Parallelism（用户设置才注入）
    if (parallelism && parallelism > 0) {
      dynamicSets.push(`SET 'parallelism.default' = '${parallelism}';`);
    }
    // 4. Restart Strategy（用户设置才注入）
    if (restartStrategy) {
      const delayMap = { 's': 's', 'ms': 'ms', 'min': 'min' };
      if (restartStrategy === 'none') {
        dynamicSets.push(`SET 'restart-strategy' = 'none';`);
      } else if (restartStrategy === 'fixed-delay') {
        dynamicSets.push(`SET 'restart-strategy' = 'fixed-delay';`);
        if (restartAttempts) dynamicSets.push(`SET 'restart-strategy.fixed-delay.attempts' = '${restartAttempts}';`);
        if (restartDelayVal) {
          const delayStr = restartDelayVal + (delayMap[restartDelayUnit] || 's');
          dynamicSets.push(`SET 'restart-strategy.fixed-delay.delay' = '${delayStr}';`);
        }
      } else if (restartStrategy === 'failure-rate') {
        dynamicSets.push(`SET 'restart-strategy' = 'failure-rate';`);
        if (restartAttempts) dynamicSets.push(`SET 'restart-strategy.failure-rate.max-failures-per-interval' = '${restartAttempts}';`);
        if (restartDelayVal) {
          const delayStr = restartDelayVal + (delayMap[restartDelayUnit] || 's');
          dynamicSets.push(`SET 'restart-strategy.failure-rate.failure-rate-interval' = '${delayStr}';`);
          dynamicSets.push(`SET 'restart-strategy.failure-rate.delay' = '${delayStr}';`);
        }
      }
    }
    // 5. 资源参数（用户面板设置时才注入）
    if (jmMemory) dynamicSets.push(`SET 'jobmanager.memory.process.size' = '${jmMemory}';`);
    if (jmCpu) dynamicSets.push(`SET 'process.jobmanager.cores' = '${jmCpu}';`);
    if (tmMemory) dynamicSets.push(`SET 'taskmanager.memory.process.size' = '${tmMemory}';`);
    if (tmCpu) dynamicSets.push(`SET 'process.taskmanager.cores' = '${tmCpu}';`);
    if (tmSlots) dynamicSets.push(`SET 'taskmanager.numberOfTaskSlots' = '${tmSlots}';`);

    // ★ 最终 SQL：TABLEAU 模式 + 动态参数 + Catalog 定义 + 用户 SQL
    const allSetStmts = [
      `SET 'sql-client.execution.result-mode' = 'TABLEAU';`,
      ...dynamicSets
    ].join('\n');

    // 如果用户指定了 catalog，注入 USE CATALOG
    const useCatalogStmt = effectiveCatalog ? `\nUSE CATALOG ${safeId(effectiveCatalog)};` : '';
    const execSql = `${allSetStmts}\n${catalogStmts ? '\n' + catalogStmts + '\n': ''}${useCatalogStmt}\n${sql}`;

    const b64 = Buffer.from(execSql).toString('base64');
    execSync(`wsl.exe -e bash -c "echo '${b64}' | base64 -d > ${tmpFile} && chmod 644 ${tmpFile}"`);

    // 异步执行，输出重定向到文件，追加完成标记
    const child = exec(`wsl.exe -e bash -c "cd ${CONFIG.flink.home} && bash bin/sql-client.sh -f ${tmpFile} > ${outFile} 2>&1; echo '==EXEC_DONE==' >> ${outFile}"`);
    child.on('error', (err) => { console.error('Flink execute error:', err.message); });

    // 轮询等待结果：批模式 60 秒 / 流模式 300 秒
    const isStreaming = execMode === 'STREAMING';
    const maxWait = isStreaming ? 300 : 60;
    let output = '';
    let done = false;
    for (let i = 0; i < maxWait; i++) {
      await new Promise(r => setTimeout(r, 1000));
      try {
        const out = execSync(`wsl.exe -e bash -c "cat ${outFile} 2>/dev/null || echo ''"`, { timeout: 5000 }).toString();
        if (out.includes('==EXEC_DONE==')) {
          output = out.replace('==EXEC_DONE==', '').trim();
          done = true;
          break;
        }
        output = out;
      } catch (e) {}
    }

    // 清理临时文件
    try { exec(`wsl.exe -e bash -c "rm -f ${tmpFile} ${outFile}"`, () => {}); } catch (e) {}

    if (!done && !output) {
      return res.json({ success: false, error: `执行超时（${maxWait}秒）或 Flink SQL Client 未响应` });
    }

    // 过滤掉启动日志，精确定位 TABLEAU 表格边界
    // 策略：多语句时（CREATE CATALOG; USE; SELECT）DDL 不产生表格，
    // 只有最后的 SELECT 输出 TABLEAU 格式 → 从后往前找最后一个有效表格
    const lines = output.split('\n');
    let resultStart = -1, resultEnd = -1;
    const isBorder = (s) => /\+[-+]+\+/.test(s); // 匹配 +--------+ 分隔线
    const isDataRow = (s) => s.trim().startsWith('|') && !s.includes('Flink SQL');

    // 收集所有可能的表格段落（支持多语句场景：DDL + 最终 SELECT）
    const tableSegments = [];
    let segStart = -1;
    for (let i = 0; i < lines.length; i++) {
      if (isBorder(lines[i])) {
        if (segStart === -1) segStart = i;
        else {
          // 找到成对分隔线，记录一个完整段落
          tableSegments.push({ start: segStart, end: i });
          segStart = i; // 当前行作为下一段的起点
        }
      }
      // 如果在分隔线之后遇到非分隔线/非数据行，且已有段起点
      if (segStart >= 0 && !isBorder(lines[i]) && !isDataRow(lines[i]) && !/^\s*$/.test(lines[i])) {
        // 检查是否已经过了表头区域（>3行非表格内容则认为段落结束）
        if (i - segStart > 3) segStart = -1;
      }
    }
    // 处理未闭合的最后一段（没有结束分隔线但有数据行直到文件末尾）
    if (segStart >= 0) {
      // 往前找最后一个有数据的行
      let lastDataRow = lines.length - 1;
      for (let j = lines.length - 1; j >= segStart; j--) {
        if (isDataRow(lines[j]) || isBorder(lines[j])) { lastDataRow = j; break; }
      }
      if (lastDataRow > segStart) tableSegments.push({ start: segStart, end: lastDataRow });
    }

    // 优先使用最后一段（多语句时是最终 SELECT 的结果；单语句时就是唯一的一段）
    if (tableSegments.length > 0) {
      const lastSeg = tableSegments[tableSegments.length - 1];
      resultStart = lastSeg.start;
      resultEnd = lastSeg.end;
    }

    // fallback：如果上面没找到任何段落，尝试旧逻辑（兼容单条简单 SELECT）
    if (resultStart < 0) {
      for (let i = 0; i < lines.length; i++) {
        if (isDataRow(lines[i])) { resultStart = i; break; }
      }
      if (resultStart >= 0) {
        for (let i = lines.length - 1; i >= resultStart; i--) {
          if (isDataRow(lines[i])) { resultEnd = i; break; }
        }
      }
    }
    let cleanOutput = resultStart >= 0 && resultEnd >= resultStart
      ? lines.slice(resultStart, resultEnd + 1).join('\n')
      : output;
    // 去掉每行开头的 Flink SQL> 提示符
    cleanOutput = cleanOutput.split('\n').map(l => l.replace(/^Flink SQL>\s*/, '')).join('\n');

    // 统计数据行数（过滤掉分隔线、列名行和空行）
    let rowCount = 0;
    if (resultStart >= 0) {
      const dataLines = cleanOutput.split('\n').filter(l => l.trim().startsWith('|') && !/^\|[\s-]+\|/.test(l.trim()));
      rowCount = Math.max(0, dataLines.length - 1); // 减1去掉表头行
    }

    res.json({ success: true, output: cleanOutput, isTable: resultStart >= 0, timeout: !done, mode: execMode, rowCount });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 提交 SQL 作业（增强版：同步等待 + 作业确认）
app.post('/api/flink/submit', async (req, res) => {
  const { sql, type, catalogId } = req.body;
  if (!sql) return res.json({ success: false, error: 'SQL 不能为空' });
  try {
    // ★ 同名任务全局唯一校验：提取作业名，检查是否已存在同名任务
    const pipelineNameMatch = sql.match(/SET\s+['"]pipeline\.name['"]\s*=\s*['"]([^'"]+)['"]/i);
    const inferredJobName = pipelineNameMatch ? pipelineNameMatch[1] : null;
    if (inferredJobName) {
      const existingHistory = getJobHistory();
      const duplicate = Object.values(existingHistory).find(
        j => j.name === inferredJobName && j.state !== 'FINISHED' && j.state !== 'FAILED' && j.state !== 'CANCELED'
      );
      if (duplicate) {
        return res.json({
          success: false,
          error: `⚠️ 同名任务 "${inferredJobName}" 已存在（状态: ${duplicate.state}，ID: ${duplicate.id}）。请先取消或删除已有任务后再提交新任务。如需恢复暂停的任务，请使用「继续」按钮。`,
          duplicate: { id: duplicate.id, name: duplicate.name, state: duplicate.state }
        });
      }
    }

    const ts = Date.now();
    const tmpFile = `/tmp/lakehouse_submit_${ts}.sql`;
    const logFile = `/tmp/lakehouse_submit_${ts}.log`;
    // 保存 SQL 到 store（inferredJobName 已在上面提取）
    saveSqlToStore(sql, inferredJobName);

    // 构建 Flink SQL：前置 Paimon Catalog 配置
    const catalogs = getCatalogs();
    const flinkCats = Object.values(catalogs).filter(c => c.type === 'paimon' && (c.engine === 'flink' || c.engine === 'both'));
    const catalogStmts = flinkCats.map(c => {
      const w = c.props.warehouse || '/mnt/g/paimon_warehouse';
      const wPath = w.startsWith('/') ? w : '/' + w;
      return `CREATE CATALOG IF NOT EXISTS ${c.id} WITH (
  'type' = 'paimon',
  'paimon.catalog.type' = '${c.props.paimon_catalog_type || 'filesystem'}',
  'warehouse' = 'file://${wPath}'
);`;
    }).join('\n');

    // 如果指定了 catalog，注入 USE CATALOG
    const submitCatalog = req.body.catalog || null;
    const useCatalogForSubmit = submitCatalog ? `\nUSE CATALOG ${safeId(submitCatalog)};\n` : '';
    const fullSql = catalogStmts ? `${catalogStmts}\n${useCatalogForSubmit}${sql}` : (useCatalogForSubmit + sql);

    // 格式化 SQL：确保每条语句独立一行（Flink SQL Client -f 不支持单行多语句）
    const formattedSql = fullSql
      .split('\n')
      .map(line => {
        // 将分号分隔的多语句拆分为多行（保留 CREATE/CASE/SET 等上下文完整性）
        if ((line.match(/;/g) || []).length > 1 && !line.trim().startsWith('--')) {
          return line.split(/;\s*/).filter(s => s.trim()).map(s => s.trim() + ';').join('\n');
        }
        return line;
      })
      .join('\n');

    // 用 base64 编码绕过所有字符转义问题
    const b64 = Buffer.from(formattedSql).toString('base64');
    execSync(`wsl.exe -e bash -c "echo '${b64}' | base64 -d > ${tmpFile} && chmod 644 ${tmpFile}"`);

    // 判断是否为流式/长时间运行作业（CDC、INSERT INTO 等）
    const isStreamingJob = /(?:INSERT\s+INTO|CREATE\s+TABLE.*?(?:cdc|kafka|paimon|datagen|mysql-cdc))/si.test(fullSql)
      || (type === 'streaming')
      || /execution\.runtime-mode.*?STREAMING/si.test(fullSql);

    // 记录提交前的作业列表（用于比对新生成的作业）
    let jobsBefore = [];
    try { jobsBefore = (await fetchFlink('/jobs/overview')).jobs || []; } catch (e) {}

    if (isStreamingJob) {
      // ===== 流式作业：后台启动（unref 避免阻塞）+ 多策略检测新作业 =====
      const child = exec(
        `wsl.exe -e bash -c "cd ${CONFIG.flink.home} && bash bin/sql-client.sh -f ${tmpFile} > ${logFile} 2>&1"`,
        { detached: true, timeout: 0 }
      );
      child.unref(); // 让子进程独立运行，不阻塞事件循环
      child.on('error', (err) => console.error('Submit streaming spawn err:', err.message));

      // ★ 改造：收集所有新 Job ID（Flink 对多 DML SQL 会产生多个 Job）
      // 策略: 轮询 Flink REST API 检测新 job，连续 5 秒无新 job 则认为收集完毕
      const detectedJobIds = [];
      let noNewCount = 0;
      for (let i = 0; i < 90; i++) {
        await new Promise(r => setTimeout(r, 1000));
        try {
          const jobsAfter = await fetchFlink('/jobs/overview');
          const afterIds = new Set((jobsAfter.jobs || []).map(j => j.jid));
          const beforeIds = new Set(jobsBefore.map(j => j.jid));
          let foundNew = false;
          for (const jid of afterIds) {
            if (!beforeIds.has(jid) && !detectedJobIds.includes(jid)) {
              detectedJobIds.push(jid);
              foundNew = true;
              noNewCount = 0; // 重置计数器
            }
          }
          if (!foundNew) {
            noNewCount++;
            if (detectedJobIds.length > 0 && noNewCount >= 5) break; // 连续 5s 无新 job，收集完成
          }

          // 从日志中提取 Job ID（补充检测）
          if (i % 5 === 4 && i > 10) {
            try {
              const logContent = execSync(`wsl.exe -e bash -c "cat ${logFile} 2>/dev/null || echo ''"`, { timeout: 3000 }).toString();
              const logJobIds = [...logContent.matchAll(/Job ID:\s*([a-f0-9]{32})/g)].map(m => m[1]);
              for (const ljid of logJobIds) {
                if (!detectedJobIds.includes(ljid)) {
                  detectedJobIds.push(ljid);
                  noNewCount = 0;
                }
              }
            } catch (e) {}
          }
        } catch (e) {}
      }

      // ★ 生成批次 ID：一次提交 = 一条记录
      const batchId = 'batch_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);
      const primaryJobId = detectedJobIds[0] || null;

      if (detectedJobIds.length > 0) {
        // 记录作业历史 — 1 条聚合记录
        const history = getJobHistory();
        history[batchId] = {
          id: batchId,
          name: inferredJobName || primaryJobId || batchId,
          sql: formattedSql,
          startTime: new Date().toISOString(),
          state: 'RUNNING',
          type: 'streaming',
          logFile,
          flinkJobIds: detectedJobIds,       // 关联的所有 Flink Job
          primaryJobId: primaryJobId,          // 主 Job（第一个，通常 INSERT INTO）
          _isBatch: true,                      // 标记为聚合记录
        };
        saveJobHistory(history);

        // ★ Task #124: 提交成功后自动采集血缘并持久化
        try {
          const sqlForLineage = formattedSql || sql || '';
          lineageAddRecord({
            batchId: batchId,
            jobId: primaryJobId,
            jobName: inferredJobName || primaryJobId || batchId,
            flinkJobIds: detectedJobIds,
            primaryJobId,
            state: 'RUNNING',
            mode: 'streaming',
            sql: sqlForLineage,
            sourceTables: extractSourceTables(sqlForLineage),
            sinkTables: extractSinkTables(sqlForLineage),
          });
        } catch (e) { logError('lineage-auto-collect', e.message); }

        res.json({
          success: true,
          message: `✅ 流式作业已成功提交并检测到 ${detectedJobIds.length} 个 Flink Job`,
          jobId: batchId,
          primaryJobId: primaryJobId,
          flinkJobCount: detectedJobIds.length,
          flinkJobIds: detectedJobIds,
          tmpFile, logFile, catalogCount: flinkCats.length,
          mode: 'streaming'
        });
      } else {
        // 即使超时未检测到，也要读取最终日志进行判断
        const batchId = 'batch_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);
        let logContent = '';
        try {
          logContent = execSync(`wsl.exe -e bash -c "cat ${logFile} 2>/dev/null || echo ''"`, { timeout: 3000 }).toString();
        } catch(e) {}
        const extractedJobIds = [...logContent.matchAll(/Job ID:\s*([a-f0-9]{32})/g)].map(m => m[1]);
        const allIds = detectedJobIds.length > 0 ? detectedJobIds : extractedJobIds;
        const primaryJobId = allIds[0] || null;

        if (allIds.length > 0) {
          // 记录作业历史 — 仍然是 1 条聚合记录
          const history = getJobHistory();
          history[batchId] = {
            id: batchId,
            name: inferredJobName || primaryJobId || batchId,
            sql: formattedSql,
            startTime: new Date().toISOString(),
            state: 'RUNNING',
            type: 'streaming',
            logFile,
            flinkJobIds: allIds,
            primaryJobId: primaryJobId,
            _isBatch: true,
          };
          saveJobHistory(history);

          // ★ Task #124: 超时 fallback 路径也采集血缘
          try {
            const sqlForLineage = formattedSql || sql || '';
            lineageAddRecord({
              batchId, jobId: primaryJobId,
              jobName: inferredJobName || primaryJobId || batchId,
              flinkJobIds: allIds, primaryJobId,
              state: 'RUNNING', mode: 'streaming',
              sql: sqlForLineage,
              sourceTables: extractSourceTables(sqlForLineage),
              sinkTables: extractSinkTables(sqlForLineage),
            });
          } catch (e) { logError('lineage-auto-collect-timeout', e.message); }

          res.json({
            success: true,
            message: `✅ 流式作业已提交（从日志检测到 ${allIds.length} 个 Job ID）`,
            jobId: batchId,
            primaryJobId: primaryJobId,
            flinkJobCount: allIds.length,
            flinkJobIds: allIds,
            tmpFile, logFile, catalogCount: flinkCats.length,
            mode: 'streaming'
          });
        } else {
          // ★ 修复 BUG：即使未检测到 Job ID，也要记录 history 和血缘（best-effort）
          // 作业可能因 JVM 冷启动慢导致 90s 超时，但实际已在 Flink 中运行
          const history = getJobHistory();
          history[batchId] = {
            id: batchId,
            name: inferredJobName || batchId,
            sql: formattedSql,
            startTime: new Date().toISOString(),
            state: 'RUNNING',
            type: 'streaming',
            logFile,
            flinkJobIds: [],
            primaryJobId: null,
            _isBatch: true,
            _detectionTimeout: true, // 标记：检测超时，后续由轮询补全
          };
          saveJobHistory(history);

          // ★ 血缘采集（best-effort：无 jobID 但有 SQL，仍然可以提取表关系）
          try {
            const sqlForLineage = formattedSql || sql || '';
            lineageAddRecord({
              batchId: batchId,
              jobId: null,
              jobName: inferredJobName || batchId,
              flinkJobIds: [],
              primaryJobId: null,
              state: 'RUNNING',
              mode: 'streaming',
              sql: sqlForLineage,
              sourceTables: extractSourceTables(sqlForLineage),
              sinkTables: extractSinkTables(sqlForLineage),
              _detectionTimeout: true,
            });
          } catch (e) { logError('lineage-auto-collect-timeout-empty', e.message); }

          res.json({
            success: true,
            message: '⚠️ SQL 已执行但未能在 90s 内通过 API 检测到新作业（JVM 启动较慢或语法问题）',
            jobId: batchId,
            tmpFile, logFile, logPreview: logContent.substring(0, 1500),
            catalogCount: flinkCats.length,
            mode: 'streaming',
            warning: '请查看 Flink Web UI (http://localhost:8081) 确认作业状态'
          });
        }
        // 异步清理临时文件（不阻塞响应）
        try { exec(`wsl.exe -e bash -c "rm -f ${tmpFile}"`, () => {}); } catch (e) {}
      }
    } else {
      // ===== 批作业：同步执行，等待完成 =====
      const child = exec(
        `wsl.exe -e bash -c "cd ${CONFIG.flink.home} && bash bin/sql-client.sh -f ${tmpFile} > ${logFile} 2>&1; echo '==SUBMIT_DONE==' >> ${logFile}"`,
        (err) => { if (err) console.error('Submit batch error:', err.message); }
      );
      child.on('error', (err) => { console.error('Submit batch spawn error:', err.message); });

      // 轮询等待完成（最多 120 秒批作业超时）
      let done = false;
      for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 1000));
        try {
          const out = execSync(`wsl.exe -e bash -c "tail -5 ${logFile} 2>/dev/null || echo ''"`, { timeout: 5000 }).toString();
          if (out.includes('==SUBMIT_DONE==')) { done = true; break; }
        } catch (e) {}
      }

      // 读取输出日志
      let logContent = '';
      try { logContent = execSync(`wsl.exe -e bash -c "cat ${logFile} 2>/dev/null || echo ''"`, { timeout: 5000 }).toString(); } catch (e) {}

      // 尝试检测新生成的作业 ID
      // ★ 收集所有新 Job ID（批作业也可能产生多个 Flink Job）
      let detectedJobIds = [];
      try {
        const jobsAfter = await fetchFlink('/jobs/overview');
        const afterIds = (jobsAfter.jobs || []).map(j => j.jid);
        const beforeIds = jobsBefore.map(j => j.jid);
        detectedJobIds = afterIds.filter(id => !beforeIds.includes(id));
        // 也从日志提取补充
        const logJobIds = [...logContent.matchAll(/Job ID:\s*([a-f0-9]{32})/g)].map(m => m[1]);
        for (const ljid of logJobIds) {
          if (!detectedJobIds.includes(ljid)) detectedJobIds.push(ljid);
        }
      } catch (e) {}

      // 记录作业历史 — 1 条聚合记录
      const history = getJobHistory();
      const primaryJobId = detectedJobIds[0] || null;
      const batchIdForBatch = 'batch_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);
      const finalState = done ? 'FINISHED' : (logContent.includes('Error') ? 'FAILED' : (detectedJobIds.length > 0 ? 'RUNNING' : 'UNKNOWN'));
      const jobEntry = {
        id: batchIdForBatch,
        name: inferredJobName || primaryJobId || `批作业 ${ts}`,
        sql: formattedSql,
        startTime: new Date().toISOString(),
        endTime: done ? new Date().toISOString() : undefined,
        state: finalState,
        type: 'batch',
        logFile,
        flinkJobIds: detectedJobIds,
        primaryJobId: primaryJobId,
        _isBatch: true,
      };
      history[batchIdForBatch] = jobEntry;
      saveJobHistory(history);

      // ★ Task #124: 批作业提交后也采集血缘
      try {
        const sqlForLineage = formattedSql || sql || '';
        lineageAddRecord({
          batchId: batchIdForBatch,
          jobId: primaryJobId,
          jobName: inferredJobName || primaryJobId || `批作业 ${ts}`,
          flinkJobIds: detectedJobIds,
          primaryJobId,
          state: finalState,
          mode: 'batch',
          sql: sqlForLineage,
          sourceTables: extractSourceTables(sqlForLineage),
          sinkTables: extractSinkTables(sqlForLineage),
        });
      } catch (e) { logError('lineage-auto-collect-batch', e.message); }

      res.json({
        success: done || logContent.includes('Job submitted') || detectedJobIds.length > 0,
        message: done ? '✅ 批作业已执行完成' : (!done && detectedJobIds.length > 0) ? `✅ 批作业已提交（检测到 ${detectedJobIds.length} 个 Flink Job）` : '⚠️ 执行完成但可能包含错误，请检查日志',
        jobId: batchIdForBatch,
        primaryJobId: primaryJobId,
        flinkJobCount: detectedJobIds.length,
        flinkJobIds: detectedJobIds,
        tmpFile, logFile,
        logPreview: logContent.substring(0, 2000),
        catalogCount: flinkCats.length,
        mode: 'batch',
        timeout: !done
      });
      // 异步清理临时文件（日志保留用于调试）
      try { exec(`wsl.exe -e bash -c "rm -f ${tmpFile}"`, () => {}); } catch (e) {}
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Flink: 获取作业关联的 SQL（从最近 store 中取）
app.get('/api/flink/jobs/:jobId/sql', (req, res) => {
  const sql = getSqlFromStore(req.params.jobId);
  res.json({ success: true, sql: sql || '' });
});

// Flink: 提交日志（读取最新的 submit log）
app.get('/api/flink/submit/log', (req, res) => {
  try {
    // 找最新的 submit log 文件
    const logFile = execSync(`wsl.exe -e bash -c "ls -t /tmp/lakehouse_submit_*.log 2>/dev/null | head -1"`).toString().trim();
    if (!logFile) return res.json({ success: true, log: '暂无日志' });
    const log = execSync(`wsl.exe -e bash -c "cat ${logFile} 2>/dev/null || echo '日志文件为空'"`).toString();
    res.json({ success: true, log: log || '日志为空', logFile });
  } catch (e) {
    res.json({ success: false, log: e.message });
  }
});

// ==================== Doris API ====================
const getDorisConn = () => mysql.createConnection({
  host: CONFIG.doris.host,
  port: CONFIG.doris.port,
  user: CONFIG.doris.user,
  password: CONFIG.doris.password,
  connectTimeout: 5000, // 5s 连接超时
  multipleStatements: false,
});

// Doris: 健康检查
app.get('/api/doris/status', async (req, res) => {
  let conn;
  try {
    conn = await getDorisConn();
    // 用 Promise.race 给查询加超时，防止 Doris 半死不活
    await Promise.race([
      conn.query('SELECT 1'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Doris 查询超时 (5s)')), 5000))
    ]);
    res.json({ success: true, status: 'connected' });
  } catch (e) {
    res.json({ success: false, status: 'disconnected', error: e.message });
  } finally {
    if (conn) conn.end();
  }
});

// Doris: 执行查询
app.post('/api/doris/query', async (req, res) => {
  const { sql, catalog } = req.body;
  if (!sql) return res.json({ success: false, error: 'SQL 不能为空' });
  let conn;
  try {
    conn = await getDorisConn();
    // 自动 REFRESH CATALOG（解决元数据缓存问题）
    if (catalog) {
      if (!validateIdentifier(catalog, 'catalog')) return res.json({ success: false, error: 'Catalog 名称包含非法字符' });
      try {
        await conn.query(`REFRESH CATALOG ${safeId(catalog)}`);
      } catch (e) { /* catalog 可能不存在，忽略 */ }
      await conn.query(`SWITCH ${safeId(catalog)}`);
    }
    const [rows, fields] = await conn.query(sql);
    const columns = fields ? fields.map(f => ({ name: f.name, type: f.type })) : [];
    res.json({ success: true, data: rows, columns, rowCount: Array.isArray(rows) ? rows.length : 0 });
  } catch (e) {
    res.json({ success: false, error: e.message });
  } finally {
    if (conn) conn.end();
  }
});

// Doris: 数据库列表
app.get('/api/doris/databases', async (req, res) => {
  const { catalog } = req.query;
  let conn;
  try {
    conn = await getDorisConn();
    if (catalog) {
      if (!validateIdentifier(catalog, 'catalog')) return res.json({ success: false, error: 'Catalog 名称包含非法字符', data: [] });
      await conn.query(`SWITCH ${safeId(catalog)}`);
    }
    const [rows] = await conn.query('SHOW DATABASES');
    res.json({ success: true, data: rows.map(r => Object.values(r)[0]) });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  } finally {
    if (conn) conn.end();
  }
});

// Doris: Catalog 列表
app.get('/api/doris/catalogs', async (req, res) => {
  let conn;
  try {
    conn = await getDorisConn();
    const [rows] = await conn.query('SHOW CATALOGS');
    res.json({ success: true, data: rows.map(r => r.CatalogName || r.Name || Object.values(r)[0]) });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  } finally {
    if (conn) conn.end();
  }
});

// Doris: 表列表
app.get('/api/doris/tables', async (req, res) => {
  const { catalog, database } = req.query;
  let conn;
  try {
    conn = await getDorisConn();
    if (catalog) {
      if (!validateIdentifier(catalog, 'catalog')) return res.json({ success: false, error: 'Catalog 名称包含非法字符', data: [] });
      await conn.query(`SWITCH ${safeId(catalog)}`);
    }
    if (database) {
      if (!validateIdentifier(database, 'database')) return res.json({ success: false, error: '数据库名包含非法字符', data: [] });
      await conn.query(`USE ${safeId(database)}`);
    }
    const [rows] = await conn.query('SHOW TABLES');
    res.json({ success: true, data: rows.map(r => Object.values(r)[0]) });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  } finally {
    if (conn) conn.end();
  }
});

// ==================== Paimon API ====================
// WSL 命令执行（带输入校验和超时保护）
const runWsl = (cmd, timeout = 10000) => {
  // 安全检查：阻止命令注入（反引号、子shell、分号链式命令）
  // 注意：不拦截 | (pipe)、$ (变量)、{ } (brace expansion)、\n\r
  // 因为内部构建的合法 shell 命令需要这些（如 || echo '{}', ${var}）
  const dangerousPatterns = /[;`]/;
  if (dangerousPatterns.test(cmd)) {
    const err = new Error(`WSL command rejected (security): contains dangerous characters`);
    err.code = 'WSL_SECURITY';
    throw err;
  }
  return execSync(`wsl.exe -e bash -c "${cmd.replace(/"/g, '\\"')}"`, { timeout }).toString().trim();
};

// 字节数格式化
const formatBytes = (bytes) => {
  if (!bytes || bytes === 0) return '0 B';
  if (typeof bytes === 'string') return bytes;
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
};

// Paimon: 数据库列表
app.get('/api/paimon/databases', (req, res) => {
  try {
    const output = runWsl(`ls ${CONFIG.paimon.warehouse}/ 2>/dev/null`);
    const dbs = output.split('\n').filter(d => d.trim().endsWith('.db')).map(d => d.trim().replace(/\.db$/, ''));
    res.json({ success: true, data: dbs });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  }
});

// Paimon: 表列表
app.get('/api/paimon/tables', (req, res) => {
  const { database } = req.query;
  if (!database) return res.json({ success: false, error: 'database 参数必填' });
  try {
    const dbPath = `${CONFIG.paimon.warehouse}/${database}.db`;
    const output = runWsl(`ls ${dbPath}/ 2>/dev/null`);
    const tables = output.split('\n').filter(Boolean);
    res.json({ success: true, data: tables });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  }
});

// Paimon: 表详情（schema + snapshot）
app.get('/api/paimon/table-detail', (req, res) => {
  const { database, table } = req.query;
  if (!database || !table) return res.json({ success: false, error: '参数不完整' });
  try {
    // 安全校验：防止路径遍历
    safeId(database, 'database');
    safeId(table, 'table');
    const tablePath = `${CONFIG.paimon.warehouse}/${database}.db/${table}`;
    // Schema
    const schemaRaw = runWsl(`cat ${tablePath}/schema/schema-0 2>/dev/null || echo '{}'`);
    let schema = {};
    try { schema = JSON.parse(schemaRaw); } catch (e) { schema = { raw: schemaRaw }; }
    // Snapshot 数量（排除软链接文件）
    const snapshotCount = runWsl(`ls -l ${tablePath}/snapshot/ 2>/dev/null | grep -v '^l' | grep 'snapshot-' | wc -l`);
    // 数据文件大小
    const totalSize = runWsl(`du -sh ${tablePath}/ 2>/dev/null | cut -f1`);
    // 最新 snapshot 内容（找最大编号的 snapshot-N 文件）
    const latestSnap = runWsl(`ls ${tablePath}/snapshot/ 2>/dev/null | grep '^snapshot-' | sort -t- -k2 -n | tail -1`);
    let snapDetail = {};
    if (latestSnap) {
      const snapRaw = runWsl(`cat ${tablePath}/snapshot/${latestSnap} 2>/dev/null || echo '{}'`);
      try { snapDetail = JSON.parse(snapRaw); } catch (e) { snapDetail = { raw: snapRaw.substring(0, 500) }; }
    }
    res.json({
      success: true,
      data: {
        schema,
        snapshotCount: parseInt(snapshotCount) || 0,
        latestSnapshot: latestSnap,
        latestSnapshotDetail: snapDetail,
        totalSize,
        path: tablePath,
      }
    });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// Paimon: 目录结构
app.get('/api/paimon/files', (req, res) => {
  const { database, table } = req.query;
  if (!database || !table) return res.json({ success: false, error: '参数不完整' });
  try {
    safeId(database, 'database');
    safeId(table, 'table');
    const tablePath = `${CONFIG.paimon.warehouse}/${database}.db/${table}`;
    const output = runWsl(`find ${tablePath} -maxdepth 3 -type f 2>/dev/null | head -50`);
    const files = output.split('\n').filter(Boolean).map(f => {
      const parts = f.replace(tablePath + '/', '').split('/');
      return { path: f.replace(tablePath + '/', ''), name: parts[parts.length - 1], dir: parts.slice(0, -1).join('/') };
    });
    res.json({ success: true, data: files });
  } catch (e) {
    res.json({ success: false, error: e.message, data: [] });
  }
});

// ==================== 监控指标聚合 API ====================
// GET /api/monitor/cluster-stats
// 返回 Flink / Doris / Paimon 的集群指标（供监控面板使用）
app.get('/api/monitor/cluster-stats', async (req, res) => {
  const result = { flink: null, doris: null, paimon: null };

  // ---- Flink ----
  try {
    const overview = await fetchFlink('/overview');
    const tms = await fetchFlink('/taskmanagers/');
    const tmList = tms.taskmanagers || [];
    const jmHeap = overview.JMHeapMemory ? formatBytes(overview.JMHeapMemory) : '–';
    // taskHeap 单位是 MB，physicalMemory 和 freeMemory 单位是 bytes
    const tmHeapTotal = tmList.reduce((sum, tm) => {
      return sum + (tm.memoryConfiguration?.taskHeap || 0);
    }, 0);
    result.flink = {
      jobsRunning: overview['jobs-running'] || 0,
      jobsFinished: overview['jobs-finished'] || 0,
      jobsFailed: overview['jobs-failed'] || 0,
      tmCount: tmList.length || overview['taskmanager-count'] || 0,
      slotsTotal: overview['slots-total'] || 0,
      slotsAvailable: overview['slots-available'] || 0,
      jmHeap: jmHeap,
      tmHeapTotal: formatBytes(tmHeapTotal),
      cpuCores: tmList.reduce((s, tm) => s + (tm.hardware?.cpuCores || 0), 0),
      tmDetail: tmList.map(tm => ({
        id: tm.id,
        ip: (tm.id || '').split(':')[0] || 'localhost',
        taskHeap: tm.memoryConfiguration?.taskHeap ? formatBytes(tm.memoryConfiguration.taskHeap) : '–',
        managedMemory: tm.memoryConfiguration?.managedMemory ? formatBytes(tm.memoryConfiguration.managedMemory) : '–',
        physicalMemory: tm.hardware?.physicalMemory ? formatBytes(tm.hardware.physicalMemory) : '–',
        cpuCores: tm.hardware?.cpuCores || 0,
        numSlots: tm.slotsNumber || 0,
        freeSlots: tm.freeSlots || 0,
      }))
    };
  } catch (e) {
    result.flink = { error: e.message };
  }

  // ---- Doris ----
  let dorisConn = null;
  try {
    dorisConn = await getDorisConn();
    // FE 节点（带查询超时）
    let feRows = [];
    try {
      [feRows] = await Promise.race([
        dorisConn.query('SHOW FRONTENDS'),
        new Promise((_, reject) => setTimeout(() => reject(new Error('Doris SHOW FRONTENDS 超时')), 5000))
      ]);
    } catch (e) { /* ignore */ }
    // BE 节点（带查询超时）
    let beRows = [];
    try {
      [beRows] = await Promise.race([
        dorisConn.query('SHOW BACKENDS'),
        new Promise((_, reject) => setTimeout(() => reject(new Error('Doris SHOW BACKENDS 超时')), 5000))
      ]);
    } catch (e) { /* ignore */ }
    // 解析 FE（使用命名键，避免列顺序变化问题）
    const feList = feRows.map(r => {
      return {
        name: r.Name || '–',
        ip: r.Host || '–',
        queryPort: r.QueryPort || '–',
        editLogPort: r.EditLogPort || '–',
        role: r.Role || '–',
        isMaster: r.IsMaster === 'true',
        alive: r.Alive === 'true',
        lastHeartbeat: r.LastHeartbeat || '–',
        version: r.Version || '–',
      };
    });
    // 解析 BE
    const beList = beRows.map(r => {
      return {
        backendId: r.BackendId || '–',
        ip: r.Host || '–',
        bePort: r.BePort || '–',
        httpPort: r.HttpPort || '–',
        alive: r.Alive === 'true',
        tabletNum: r.TabletNum || 0,
        totalCapacity: r.TotalCapacity || '–',
        availCapacity: r.AvailCapacity || '–',
        usedPct: r.UsedPct || '–',
        memory: r.Memory || '–',
        cpuCores: r.CpuCores || '–',
        lastHeartbeat: r.LastHeartbeat || '–',
        tag: r.Tag || '–',
        version: r.Version || '–',
      };
    });
    result.doris = {
      feCount: feList.filter(f => f.alive).length,
      feTotal: feList.length,
      beCount: beList.filter(b => b.alive).length,
      beTotal: beList.length,
      feList,
      beList,
    };
  } catch (e) {
    result.doris = { error: e.message };
  } finally {
    if (dorisConn) dorisConn.end();
  }

  // ---- Paimon ----
  try {
    const wh = CONFIG.paimon.warehouse;
    // 数据库数量
    const dbOut = runWsl(`ls -d ${wh}/*.db 2>/dev/null | wc -l`);
    const dbCount = parseInt(dbOut.trim()) || 0;
    // 所有数据库名称
    const dbNames = runWsl(`ls -d ${wh}/*.db 2>/dev/null | xargs -I{} basename {} .db`).split('\n').filter(Boolean);
    // 表总数（每个 db 下的目录，排除 snapshot/schema/changelog/manifest/index 等子目录）
    const skipDirs = new Set(['snapshot', 'schema', 'changelog', 'manifest', 'index', 'index_Kn1', 'index_Kn2', 'index_V']);
    let tableCount = 0;
    const tableByDb = {};
    for (const db of dbNames) {
      const tOut = runWsl(`ls -1 ${wh}/${db}.db/ 2>/dev/null`);
      const tables = (tOut || '').split('\n').filter(name => name && !skipDirs.has(name));
      tableByDb[db] = tables.length;
      tableCount += tables.length;
    }
    // Warehouse 总大小
    const sizeOut = runWsl(`du -sh ${wh} 2>/dev/null | cut -f1`);
    result.paimon = {
      dbCount,
      dbNames,
      tableCount,
      tableByDb,
      totalSize: sizeOut.trim() || '–',
    };
  } catch (e) {
    result.paimon = { error: e.message };
  }

  res.json({ success: true, data: result });
});

// ==================== 任务血缘 API ====================
// GET /api/lineage（v2 — 基于持久化血缘存储，支持全状态展示）
// 数据来源：
//   1. LINEAGE_STORE (lakehouse_lineage.json) — 提交时自动采集的血缘记录（主数据源）
//   2. Flink Job Plan — 实时解析 Source/Sink 拓扑（丰富 detail）
//   3. Paimon 文件系统 / Doris Catalog — 维度表数据
// 展示所有状态作业（RUNNING / FINISHED / FAILED / CANCELED），按 startTime 降序
app.get('/api/lineage', async (req, res) => {
  const TIMEOUT_MS = 60000;
  const timer = setTimeout(() => {
    if (!res.headersSent) {
      res.json({ success: false, error: '血缘采集超时（60s），请检查 Flink/Doris/Paimon 服务状态' });
    }
  }, TIMEOUT_MS);

  const result = { pipelines: [], sources: [], paimonTables: [], dorisTables: [], errors: [] };

  // 带超时的 WSL 执行
  const runWslAsync = (cmd) => new Promise((resolve) => {
    if (/[;`]/.test(cmd)) { resolve(''); return; }
    const proc = exec(`wsl.exe -e bash -c "${cmd.replace(/"/g, '\\"')}"`, { timeout: 5000 }, (err, stdout) => {
      resolve(err ? '' : stdout.toString().trim());
    });
    proc.on('error', () => resolve(''));
  });

  try {
    // ---- 1. 加载持久化血缘记录（主数据源）----
    let lineageRecords = [];
    try { lineageRecords = getLineageStore().records || []; } catch (e) {
      result.errors.push('lineage-store: ' + e.message);
    }

    // ---- 2. 加载作业历史（用于获取非 RUNNING 作业的状态）----
    const historyMap = new Map(); // jobId -> history record
    try {
      const historyData = loadJobHistory();
      if (Array.isArray(historyData)) {
        for (const h of historyData) historyMap.set(h.jobId, h);
      }
    } catch (e) { /* ignore */ }

    // ---- 3. 并发加载维度数据 ----
    const [flinkJobsRaw, dssRaw, paimonMeta, dorisRaw] = await Promise.allSettled([
      (async () => {
        try { const d = await fetchFlink('/jobs/overview'); return d.jobs || []; } catch (e) { result.errors.push('flink: ' + e.message); return []; }
      })(),
      Promise.resolve(Object.values(getCatalogs()).filter(c => ['mysql','postgres','doris'].includes(c.type))),
      (async () => {
        try {
          const wh = CONFIG.paimon.warehouse;
          const dbOut = await runWslAsync(`ls -d ${wh}/*.db 2>/dev/null`);
          const dbDirs = dbOut ? dbOut.split('\n').filter(d => d.trim()) : [];
          const dbNames = dbDirs.map(d => d.replace(/.db$/, '').replace(/.*\//, ''));
          return { dbNames, wh };
        } catch (e) { result.errors.push('paimon dir: ' + e.message); return { dbNames: [], wh: CONFIG.paimon.warehouse }; }
      })(),
      (async () => {
        let conn = null;
        try { conn = await getDorisConn(); return { conn, catalogs: getCatalogs() }; }
        catch (e) { result.errors.push('doris conn: ' + e.message); return null; }
      })(),
    ]);

    const flinkJobs = flinkJobsRaw.status === 'fulfilled' ? flinkJobsRaw.value : [];
    const dss = dssRaw.status === 'fulfilled' ? dssRaw.value : [];
    const paimonMetaData = paimonMeta.status === 'fulfilled' ? paimonMeta.value : { dbNames: [], wh: CONFIG.paimon.warehouse };

    // ---- 数据源（Catalog JDBC 类型）----
    result.sources = (dss || []).map(c => ({
      id: c.id, name: c.name, host: c.connection?.host, database: c.connection?.database,
      port: c.connection?.port || 3306, type: c.type,
    }));

    // ---- Paimon 表（与原逻辑相同）----
    const skipDirs = new Set(['snapshot', 'schema', 'changelog', 'manifest', 'index', 'index_Kn1', 'index_Kn2', 'index_V', 'valuekind', 'value_type']);
    const paimonTablePromises = [];
    for (const dbName of paimonMetaData.dbNames) {
      const dbPath = `${paimonMetaData.wh}/${dbName}.db`;
      const lsOut = await runWslAsync(`ls -1 ${dbPath}/ 2>/dev/null`);
      const tables = (lsOut || '').split('\n').filter(name => name.trim() && !skipDirs.has(name.trim()));
      for (const table of tables) {
        const tablePath = `${dbPath}/${table.trim()}`;
        paimonTablePromises.push(
          (async () => {
            let latestSnapTime = null, recordCount = null, latestSnap = null, tableSize = '–';
            try {
              const [snapFile, size] = await Promise.all([
                runWslAsync(`ls ${tablePath}/snapshot/ 2>/dev/null | grep '^snapshot-' | sort -t- -k2 -n | tail -1`),
                runWslAsync(`du -sh ${tablePath} 2>/dev/null | cut -f1`),
              ]);
              tableSize = size || '–';
              if (snapFile) {
                latestSnap = snapFile.trim();
                const snapRaw = await runWslAsync(`cat ${tablePath}/snapshot/${latestSnap} 2>/dev/null`);
                if (snapRaw) {
                  try {
                    const sd = JSON.parse(snapRaw);
                    if (sd['timeMillis']) latestSnapTime = parseInt(sd['timeMillis']);
                    if (sd['totalRecordCount'] !== undefined) recordCount = sd['totalRecordCount'];
                  } catch (e) {}
                }
              }
            } catch (e) {}
            return { db: dbName, table: table.trim(), path: tablePath, latestSnapTime, latestSnap, recordCount, tableSize };
          })()
        );
      }
    }
    const allPaimonTables = await Promise.allSettled(paimonTablePromises);
    result.paimonTables = allPaimonTables.filter(r => r.status === 'fulfilled').map(r => r.value);

    // ---- Doris External 表（与原逻辑相同）----
    const dorisData = dorisRaw.status === 'fulfilled' ? dorisRaw.value : null;
    if (dorisData && dorisData.conn) {
      const { conn, catalogs } = dorisData;
      try {
        for (const cat of Object.values(catalogs)) {
          if ((cat.engine === 'doris' || cat.engine === 'both') && cat.type === 'paimon') {
            try {
              await conn.query(`REFRESH CATALOG ${cat.id}`);
              const [dbs] = await conn.query(`SHOW DATABASES FROM CATALOG ${cat.id}`);
              for (const dbRow of dbs) {
                const dbName = Object.values(dbRow)[0];
                try {
                  await conn.query(`SWITCH ${safeId(cat.id)}`);
                  await conn.query(`USE ${safeId(dbName)}`);
                  const [tables] = await conn.query(`SHOW TABLES`);
                  for (const tRow of tables) {
                    const tName = Object.values(tRow)[0];
                    try {
                      const [countRows] = await conn.query(`SELECT COUNT(*) as cnt FROM ${safeId(cat.id)}.${safeId(dbName)}.${safeId(tName)} LIMIT 1`);
                      result.dorisTables.push({ catalogId: cat.id, catalogName: cat.name, db: dbName, table: tName, rowCount: countRows?.[0]?.cnt ?? '?' });
                    } catch (e) {
                      result.dorisTables.push({ catalogId: cat.id, catalogName: cat.name, db: dbName, table: tName, rowCount: '?' });
                    }
                  }
                } catch (e) {}
              }
            } catch (e) {}
          }
        }
      } finally { conn.end(); }
    }

    // ---- 4. 构建 flinkApiMap（用于实时状态查找）----
    const flinkApiMap = new Map();
    for (const j of flinkJobs) flinkApiMap.set(String(j.jid || j.id), j);

    // ---- 5. 并发获取仍在 Flink 中运行的作业 Plan（仅 RUNNING 状态需要）----
    const runningFlinkJobs = flinkJobs.filter(j => j.state === 'RUNNING');
    const jobPlanPromises = runningFlinkJobs.map(async (job) => {
      const jid = job.jid || job.id;
      try {
        const plan = await fetchFlink(`/jobs/${jid}/plan`);
        return { jid, nodes: plan?.plan?.nodes || [] };
      } catch (e) { return { jid, nodes: [] }; }
    });
    const jobPlans = await Promise.all(jobPlanPromises);
    const planMap = Object.fromEntries(jobPlans.map(p => [p.jid, p]));

    // ---- 6. 构建管线列表（核心：基于持久化记录 + 实时丰富）----
    const pipelines = [];

    // ★ BUG #143 修复：全局同名去重——同一 jobName 只保留最新一条记录
    // 去重优先级：RUNNING > RESUMING > CANCELED > FINISHED > FAILED > UNKNOWN
    // 与作业管理的 resume 合并逻辑保持一致
    const DEDUP_PRIORITY = { 'RUNNING': 5, 'RESUMING': 4, 'CANCELED': 3, 'FINISHED': 2, 'FAILED': 1, 'UNKNOWN': 0 };
    const dedupedRecords = new Map(); // Map<jobName, bestRecord>
    for (const rec of lineageRecords) {
      const recName = rec.jobName || rec.batchId || rec.jobId;
      if (!recName) continue; // 无名称的记录不参与去重，直接保留
      const existing = dedupedRecords.get(recName);
      if (!existing) {
        dedupedRecords.set(recName, rec);
      } else {
        // 同名记录：比较状态优先级 + 时间戳，保留更优的
        const existingP = DEDUP_PRIORITY[existing.state] ?? 0;
        const recP = DEDUP_PRIORITY[rec.state] ?? 0;
        const existingTime = new Date(existing.startTime || existing.timestamp || 0).getTime();
        const recTime = new Date(rec.startTime || rec.timestamp || 0).getTime();
        if (recP > existingP || (recP === existingP && recTime > existingTime)) {
          dedupedRecords.set(recName, rec);
        }
      }
    }

    // 6a. 遍历去重后的血缘记录
    for (const rec of dedupedRecords.values()) {
      const recJobId = rec.jobId || rec.batchId;

      // 确定最新状态：优先 Flink API → 作业历史 → 记录自身状态
      let state = rec.state || 'UNKNOWN';
      let startTime = rec.startTime;
      let duration = null;

      // 尝试从 Flink API 获取实时状态（适用于仍在 Flink 中的作业）
      let flinkJobInfo = null;
      if (rec.flinkJobIds && Array.isArray(rec.flinkJobIds)) {
        for (const fid of rec.flinkJobIds) {
          const fjob = flinkApiMap.get(String(fid));
          if (fjob) { flinkJobInfo = fjob; break; }
        }
      } else if (rec.primaryJobId) {
        flinkJobInfo = flinkApiMap.get(String(rec.primaryJobId));
      }
      if (flinkJobInfo) {
        state = flinkJobInfo.state || state;
        startTime = flinkJobInfo['start-time'] ? new Date(flinkJobInfo['start-time']).toISOString() : startTime;
        duration = flinkJobInfo.duration ? Math.round(flinkJobInfo.duration / 1000) : null;
      } else {
        // Flink 中找不到，尝试从历史记录取状态
        const hist = historyMap.get(recJobId) || historyMap.get(rec.batchId) || historyMap.get(rec.primaryJobId);
        if (hist) {
          state = hist.state || state;
          startTime = hist.startTime || startTime;
        }
      }

      // 从 Flink Plan 提取 Source/Sink 详情（仅当有运行中的作业时）
      let sourceDetail = null, paimonSinkDetail = null, dorisSinkDetail = null, vertices = [];

      // 尝试从 Plan 解析拓扑
      const planNodes = flinkJobInfo ? (planMap[String(flinkJobInfo.jid || flinkJobInfo.id)]?.nodes || []) : [];
      let extractedPaimonSink = null, extractedSourceDbTable = null;

      for (const node of planNodes) {
        const desc = (node.description || '') + '';
        const descLower = desc.toLowerCase();
        vertices.push({
          id: node.id || '',
          name: desc.split('<br>')[0].trim().substring(0, 60),
          parallelism: node.parallelism || 0,
          fullDesc: desc.replace(/<br>/g, '\n').trim(),
        });
        if (desc.includes('TableSourceScan')) {
          const m = desc.match(/table=\[\[[^,]+,\s*([^,]+),\s*([^\]]+)\]\]/);
          if (m) extractedSourceDbTable = { database: m[1].trim(), table: m[2].trim() };
        }
        if (!extractedPaimonSink && descLower.includes('writer')) {
          const m = desc.match(/Writer\s*:\s*(\w+)/);
          if (m) extractedPaimonSink = { table: m[1].trim() };
        }
      }

      // 如果 Plan 解析失败，用提交时提取的 SQL 表名作为 fallback
      const sinkTables = rec.sinkTables || [];
      const sourceTables = rec.sourceTables || [];
      const effectivePaimonSink = extractedPaimonSink || (sinkTables.length > 0 ? { table: sinkTables[0] } : null);
      const effectiveSource = extractedSourceDbTable || (sourceTables.length > 0 ? { database: sourceTables[0], table: sourceTables[1] || sourceTables[0] } : null);

      // 匹配 Source 详情
      if (effectiveSource) {
        const md = result.sources.find(ds => ds.database === effectiveSource.database || ds.name?.toLowerCase().includes(effectiveSource.database));
        if (md) sourceDetail = { dsId: md.id, dsName: md.name, database: effectiveSource.database, table: effectiveSource.table };
        else sourceDetail = { database: effectiveSource.database, table: effectiveSource.table };
      }

      // 匹配 Paimon Sink 详情
      if (effectivePaimonSink) {
        const matched = result.paimonTables.find(t =>
          t.table === effectivePaimonSink.table ||
          t.table === effectivePaimonSink.table.replace(/_/g, '') ||
          t.table.replace(/_/g, '') === effectivePaimonSink.table
        );
        if (matched) {
          paimonSinkDetail = {
            db: matched.db, table: matched.table,
            latestSnapTime: matched.latestSnapTime ? new Date(matched.latestSnapTime).toLocaleString() : '–',
            recordCount: matched.recordCount, tableSize: matched.tableSize,
          };
        }
      }

      // 匹配 Doris 表详情
      if (effectivePaimonSink) {
        const md = result.dorisTables.find(t => t.table === effectivePaimonSink.table || t.table === effectivePaimonSink.table.replace(/_/g, ''));
        if (md) dorisSinkDetail = { db: md.db, table: md.table, rowCount: md.rowCount };
      }

      // 状态颜色映射
      const stateColorMap = { RUNNING: 'green', FINISHED: '#28a745', FAILED: 'red', CANCELED: '#6c757d', UNKNOWN: 'yellow' };

      pipelines.push({
        jobId: recJobId,
        batchId: rec.batchId || null,
        jobName: rec.jobName || rec.sqlFile || recJobId,
        state,
        stateColor: stateColorMap[state] || 'yellow',
        startTime: startTime ? new Date(startTime).toLocaleString() : '–',
        duration: duration ? duration + 's' : '–',
        sqlFile: rec.sqlFile || null,
        source: sourceDetail,
        paimonSink: paimonSinkDetail || (effectivePaimonSink ? { table: effectivePaimonSink.table, unknown: true } : null),
        dorisSink: dorisSinkDetail,
        vertices,
        _fromPersisted: true,
      });
    }

    // 6b. 兜底：仍在运行但无持久化记录的作业（向后兼容）
    const persistedJobIds = new Set(lineageRecords.map(r => r.jobId).filter(Boolean));
    for (const job of runningFlinkJobs) {
      const jid = String(job.jid || job.id);
      if (persistedJobIds.has(jid)) continue; // 已在持久化记录中

      const pInfo = planMap[jid] || { nodes: [] };
      const nodes = pInfo.nodes || [];
      let paimonSink = null, sourceDbTable = null;
      const vertices = [];

      for (const node of nodes) {
        const desc = (node.description || '') + '';
        const descLower = desc.toLowerCase();
        vertices.push({ id: node.id || '', name: desc.split('<br>')[0].trim().substring(0, 60), parallelism: node.parallelism || 0, fullDesc: desc.replace(/<br>/g, '\n').trim() });
        if (desc.includes('TableSourceScan')) {
          const m = desc.match(/table=\[\[[^,]+,\s*([^,]+),\s*([^\]]+)\]\]/);
          if (m) sourceDbTable = { database: m[1].trim(), table: m[2].trim() };
        }
        if (!paimonSink && descLower.includes('writer')) {
          const m = desc.match(/Writer\s*:\s*(\w+)/);
          if (m) paimonSink = { table: m[1].trim() };
        }
      }

      let paimonDetail = null, dorisDetail = null, sourceDetail = null;
      if (paimonSink) {
        const pm = result.paimonTables.find(t => t.table === paimonSink.table || t.table === paimonSink.table.replace(/_/g, '') || t.table.replace(/_/g, '') === paimonSink.table);
        if (pm) paimonDetail = { db: pm.db, table: pm.table, latestSnapTime: pm.latestSnapTime ? new Date(pm.latestSnapTime).toLocaleString() : '–', recordCount: pm.recordCount, tableSize: pm.tableSize };
        const dm = result.dorisTables.find(t => t.table === paimonSink.table || t.table === paimonSink.table.replace(/_/g, ''));
        if (dm) dorisDetail = { db: dm.db, table: dm.table, rowCount: dm.rowCount };
      }
      if (sourceDbTable) {
        const sm = result.sources.find(ds => ds.database === sourceDbTable.database || ds.name?.toLowerCase().includes(sourceDbTable.database));
        if (sm) sourceDetail = { dsId: sm.id, dsName: sm.name, database: sourceDbTable.database, table: sourceDbTable.table };
        else sourceDetail = { database: sourceDbTable.database, table: sourceDbTable.table };
      }

      pipelines.push({
        jobId: jid, jobName: job.name || jid, state: job.state || 'RUNNING',
        stateColor: 'green',
        startTime: job['start-time'] ? new Date(job['start-time']).toLocaleString() : '–',
        duration: job.duration ? Math.round(job.duration / 1000) + 's' : '–',
        source: sourceDetail,
        paimonSink: paimonDetail || (paimonSink ? { table: paimonSink.table, unknown: true } : null),
        dorisSink: dorisDetail, vertices,
        _fromPersisted: false,
      });
    }

    // 按 startTime 降序排序（最新的在前）
    pipelines.sort((a, b) => {
      const ta = a.startTime === '–' ? 0 : new Date(a.startTime).getTime() || 0;
      const tb = b.startTime === '–' ? 0 : new Date(b.startTime).getTime() || 0;
      return tb - ta;
    });

    result.pipelines = pipelines;
    clearTimeout(timer);
    res.json({ success: true, data: result });
  } catch (e) {
    clearTimeout(timer);
    if (!res.headersSent) res.json({ success: false, error: e.message });
  }
});

// ==================== 健康检查 ====================
app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Lakehouse Platform API Running',
    time: new Date().toISOString(),
    version: '1.0.0',
    // 仅暴露服务状态，不暴露内部地址配置
    services: {
      flink: CONFIG.flink.baseUrl ? 'configured' : 'not configured',
      doris: CONFIG.doris.host ? 'configured' : 'not configured',
      paimon: CONFIG.paimon.warehouse ? 'configured' : 'not configured',
    }
  });
});

// ==================== 全局错误处理 ====================
// 未捕获异常 — 记录日志并优雅退出（避免进程静默挂掉）
process.on('uncaughtException', (err) => {
  console.error('[FATAL] uncaughtException:', err.message, err.stack);
  // 非致命错误继续运行，致命错误（如内存不足）退出
  if (err.code === 'ENOMEM' || err.code === 'EADDRINUSE') {
    process.exit(1);
  }
});
process.on('unhandledRejection', (reason) => {
  console.warn('[WARN] unhandledRejection:', reason);
});

// 全局错误处理中间件（兜底）
app.use((err, req, res, next) => {
  console.error(`[ERROR] ${req.method} ${req.path}:`, err.message);
  // 安全错误：不暴露内部堆栈
  const statusCode = err.statusCode || 500;
  const isClientError = statusCode < 500;
  res.status(statusCode).json({
    success: false,
    error: isClientError ? err.message : '服务器内部错误',
    ...(process.env.NODE_ENV === 'development' ? { detail: err.message } : {}),
  });
});

// ==================== 启动 ====================
const PORT = parseInt(process.env.PORT) || 7071;
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Lakehouse Platform started: http://localhost:${PORT}`);
  console.log(`   Flink:  ${CONFIG.flink.baseUrl}`);
  console.log(`   Doris:  ${CONFIG.doris.host}:${CONFIG.doris.port}`);
  console.log(`   Paimon: ${CONFIG.paimon.warehouse}`);
  // 延迟初始化 Doris Catalog（等待 Doris FE 就绪）
  setTimeout(() => { initDorisCatalogs().catch(() => {}); }, 5000);
});
