# 日志采集与事件追踪平台

面向应用日志收集、事件埋点追踪、流式采集与结构化归集的多源日志采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源管理 | `log_source` | 日志源 | 源编号、源名称、日志类型 |
| 日志解析 | `log_parser` | 解析规则 | 规则编号、关联源、正则表达式 |
| 事件埋点 | `event_tracker` | 埋点事件 | 事件编号、事件名称、埋点页面 |
| 流式缓冲 | `stream_buffer` | 缓冲配置 | 配置编号、关联源、缓冲区大小 |
| 字段映射 | `field_mapping` | 映射规则 | 规则编号、源字段、目标字段 |
| 采集代理 | `agent_heartbeat` | 采集代理 | 代理编号、部署主机、代理版本 |
| 事件校验 | `event_validate` | 校验规则 | 规则编号、关联事件、字段检查 |
| 日志轮转 | `log_rotate` | 轮转策略 | 策略编号、关联源、轮转间隔 |
| 日志富化 | `enrich_join` | 富化规则 | 规则编号、目标字段、维表来源 |
| 告警规则 | `alert_rule` | 告警规则 | 规则编号、关联源、触发条件 |
| 日志分区 | `log_partition` | 分区规则 | 规则编号、关联源、分区键 |
| 采集位点 | `collect_checkpoint` | 位点记录 | 记录编号、关联源、文件偏移 |
| 窗口去重 | `dedup_window` | 去重窗口 | 窗口编号、关联源、去重键 |
| 聚合计算 | `aggregate_olap` | 聚合规则 | 规则编号、关联源、聚合维度 |
| 格式输出 | `format_output` | 输出配置 | 配置编号、关联源、输出格式 |
| 回溯采集 | `backfill_range` | 回溯任务 | 任务编号、关联源、时间范围 |
| 采样策略 | `sampling_rule` | 采样规则 | 规则编号、关联源、采样方式 |
| 质量诊断 | `log_quality` | 诊断报告 | 报告编号、关联源、解析成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 空批次质量诊断

日志解析按批次落盘，一次 `parse_log_file` 生成一个批次目录
`data/logs/batches/<源编号>/<批次号>/`，包含本批 `parsed.jsonl`、
`parse_errors.jsonl`、`manifest.json`，并更新该源的当前批次指针。
质量诊断只读取当前批次，不会把上一批的解析错误算进这一批的空批次。

诊断结论分四种，完成状态与待核状态不混用：

| 判定 | 条件 | 完成 | 成功率 |
| --- | --- | --- | --- |
| 解析完成 | 当前批次已完成且无错误 | 是 | 按本批输入计算 |
| 存在解析错误 | 当前批次已完成且有错误行 | 是 | 按本批输入计算 |
| 确认零行 | 当前批次已完成且输入 0 行 | 是 | 100 |
| 待核 | 当前批次进行中/已中断/已失败 | 否 | 空（不出具完成口径） |
| 缺产物 | 当前批次指针或本批产物缺失 | 否 | 空（不出具完成口径） |

- 历史批次的错误原行仍可通过批次接口查询，并在当前报告的 `history`
  中标注“不计入当前批次”，供值班员追溯旧异常。
- 待核/缺产物时报告给出 `retry`（输入文件、采集位点、批次号），
  并且质量告警管道会跳过告警评估，不会用历史证据或 0% 成功率补出一个完成结论。
- 当前报告固定写到 `data/exports/quality_current_<源编号>.json`，
  每次诊断覆盖发布；`data/exports/quality_reports.jsonl` 保留诊断留档。

相关接口：

```text
GET  /api/log_quality/report/{source_key}              # 读取已发布的当前报告（含 stale 标记）
POST /api/log_quality/diagnose/{source_key}            # 按当前批次重新诊断并发布
GET  /api/log_parser/batches/{source_key}              # 批次台账
GET  /api/log_parser/batches/{source_key}/{batch_id}   # 历史批次证据（含错误原行）
POST /api/log_parser/interrupt/{source_key}            # 模拟解析断开，验证待核判定
```

回归测试：`backend/test_empty_batch_quality.py`。

领域：数据工程
子方向：数据采集、爬虫与RPA
