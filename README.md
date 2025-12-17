# SQL Notebook 用户操作指引

面向 Windows 11 风格的多标签 SQL 笔记本，基于 Java 22 + Swing + RSyntaxTextArea，支持 PostgreSQL / Hive 的编辑、执行与本地元数据缓存。

## 快速开始
1. 安装 JDK 22 与 Maven，并确保可以访问 Maven Central。
2. 运行 `mvn -DskipTests package` 生成可执行 fat JAR。
3. 使用 `java -jar target/sql_client-*-jar-with-dependencies.jar` 启动应用。

> 若容器/网络限制导致构建失败，可在联网环境下重新执行 Maven 下载依赖。

## 界面布局与面板停靠
- 默认布局：左侧“对象/笔记”导航，中央 SQL 编辑器，底部停靠“结果/日志/任务/历史”多标签面板，右侧为 Inspector 预留区。
- 视图菜单：
  - “切换左侧面板 / 切换底部面板 / 切换右侧面板”可快速显示/折叠对应区域。
  - “日志停靠位置”可在底部、右侧或浮动窗口之间切换，切换时日志内容与追加写入保持不变。
  - “重置布局”恢复默认分栏、停靠位置与窗口大小。
- 工具栏：
  - 运行/停止按钮保持不变，新增“日志”快捷键可一键聚焦日志面板，“专注”可在编辑器最大化与还原之间切换。
- 布局持久化：窗口大小、分隔条位置、面板可见性、日志停靠位置会自动保存，重启后恢复；浮动日志窗口的尺寸与位置也会被记住。

## 编辑器子窗口（Editor Tab）新布局
- **三段式 UI**：顶部内置编辑器工具条（含布局预设、编辑/结果一键最大化），中部可在“分屏/标签”两种模式间切换，底部状态栏实时显示布局、当前结果集、执行状态与自动保存时间。
- **默认即双区可见**：分屏模式下 SQL 编辑区与结果区默认同时可见，结果区初始高度约占 40%，无须拖动分割条即可阅读。
- **多结果集切换**：结果区顶部的“结果集导航条”支持下拉/上一条/下一条快速跳转，同一视图中仅呈现当前结果集的表格模型，避免遮挡。
- **列宽自适应**：导航条提供“适配列宽/重置列宽”，并针对列头及前 200 行计算宽度，保障可读性。
- **布局快捷键**：`Ctrl+Alt+R` 切换结果区最大化/还原，`Ctrl+Alt+E` 切换编辑区最大化/还原；工具条的 70/30、60/40、40/60 预设可一键调整比例。
- **标签模式**：针对窄屏可切换为 Editor/Result 标签页，无需拖动即可查看结果；切换时记住上次选择的模式。

## 核心操作
### 窗口与焦点
- 支持独立窗口模式与面板模式，子窗口可拖动、最大化/最小化或平铺。
- 程序启动会恢复上次的窗口集合，并自动把焦点放在最后一个打开的子窗口。
- 工具栏的“执行/停止”按钮随当前焦点窗口的执行状态切换；切换焦点即可控制对应窗口的 SQL 请求启动或中止。

### 智能联想模式
- Alt+E 全局切换联想开关，状态栏显示“联想: 开启/关闭”，无需在光标附近弹提示气泡。
- 激活时，在输入表/视图、字段、函数或存储过程位置自动弹出联想列表，连续输入会保持弹窗开启；敲回车选择或在语句末尾输入分号会收起弹窗。

### SQL 执行
- Ctrl+Enter 会执行选中内容、光标所在语句或连续非空行块，分号拆分后串行提交，结果出现在当前窗口或底部共享结果区。
- 运行中的窗口禁用“执行”并启用“停止”，停止会中止对应的 HTTPS 请求。

### 双向链接标签
- 在正文中插入 `[[标签]]` 可建立跳转/引用关系；执行 SQL 时系统会自动忽略方括号，仅保留标签文本，不影响语句正确性。

### 元数据与状态栏
- 启动后自动刷新远端元数据，状态栏显示当前元数据数量及本次变动数；刷新失败会提示“使用本地库元数据”。
- 可通过“工具-重置元数据”清空缓存并重新拉取。

### 操作日志
- 主窗体右侧的“操作日志”默认收拢，点击分隔条或一键展开按钮可查看 SQL 执行与元数据抓取的详细请求/响应。

### 帮助
- “帮助-使用说明”提供 Markdown 格式的功能指南，重点介绍联想模式、双向链接与 SQL 块执行。

## 最近更新
- 新增 Alt+E 联想开关、元数据状态提示与日志面板默认收拢，执行/停止按钮随焦点窗口切换。

## 异步 SQL 执行前端适配说明
### 架构与数据流
- 客户端不再直接调用旧的同步 SQL 接口，而是针对 `/waf/api/jobs/*` 异步接口：编辑器 SQL 文本 → AES/CBC/PKCS5Padding 加密 → `/jobs/submit` 提交返回 jobId → 后台线程轮询 `/jobs/status` → 完成后 `/jobs/result` 拉取结果集 → `QueryResultPanel` 展示。
- 所有请求统一附带 `X-Request-Token: WAF_STATIC_TOKEN_202405`，并使用本地信任所有证书的 HttpClient 发送。

### 核心类职责
- `tools.sqlclient.exec.SqlExecutionService`：封装提交、轮询与结果获取；日志中输出 jobId、进度与耗时。
- `tools.sqlclient.exec.AsyncSqlConfig`：默认的 BASE_URL、Token、AES Key/IV 配置与 URL 构造工具。
- `tools.sqlclient.util.AesEncryptor`：负责将 SQL 明文加密成 Base64 AES 密文，供 `/jobs/submit` 使用。
- `tools.sqlclient.exec.AsyncJobStatus`：表示后台任务的状态、进度与摘要。
- `tools.sqlclient.ui.QueryResultPanel`：支持进度条、状态/耗时展示，并在任务完成后渲染结果集或错误信息。
- `tools.sqlclient.ui.AsyncJobListDialog`：展示当前后台任务列表，提供刷新与取消入口。

### UI 操作
- Ctrl+Enter 依旧执行当前 SQL 块；提交后状态栏与结果面板先显示“已提交，等待执行”，随后轮询进度并更新百分比、耗时与 jobId。
- 任务完成时，结果面板自动填充 SELECT 结果或 DDL/DML 影响行数提示；失败/取消则在同一面板展示错误信息。
- 通过菜单“视图 -> 异步任务列表”打开任务窗口，可查看 ACCEPTED/QUEUED/RUNNING/CANCELLING 中的任务并手动取消。
- 工具栏的“停止”按钮会向后端发送 `/jobs/cancel` 请求，状态栏提示“已发送取消请求”，最终状态由后续轮询决定。

### 配置覆盖
- 默认 BASE_URL/Token/AES Key/IV 定义在 `AsyncSqlConfig` 中，可按环境需要改写；如已有外部配置中心，可在初始化时覆盖这些常量。
- 仍复用已有的 WAF 域名/证书忽略策略，HttpClient 创建逻辑保持不变。

### 调试与常见问题
- 操作日志（OperationLog）会输出 jobId、提交/状态轮询/完成时的进度记录，便于排查长任务。
- 若响应 `success:false` 或 HTTP 非 200，客户端会在结果面板展示错误提示且不中断 UI。
- Token 错误或网络异常时，日志与状态栏会提示失败原因；SQL 本身失败会在结果面板呈现后端返回的 errorMessage。

## 前端开发技术文档：元数据获取与列顺序

### 元数据获取方式
- 统一通过 POST 提交 SQL 方式获取，复用 `AsyncSqlConfig` 的 BASE_URL、`X-Request-Token` Header 与忽略证书的 HttpClient 封装。
- 使用 `SqlExecutionService.executeSync(sql)` 同步执行信息_schema 查询，返回的列名与行数据保持服务端顺序。
- 默认 schema 固定为 `leshan`，不再从远端枚举 schema，树形结构的根 schema 亦固定。

### 拉取的元数据 SQL
- 表（BASE TABLE）：`information_schema.tables` 按 `table_name` 排序。
- 视图：`information_schema.views` 按 `table_name` 排序。
- 字段：`information_schema.columns`，按 `ordinal_position` 排序并写入本地 SQLite 的 `sort_no` 字段，用于 UI 展示与差分比较。

### 列顺序保证策略
- 优先使用服务端返回的列名数组：依次读取 `columns`、`resultColumns`、`columnNames`，按后端给出的顺序直接渲染。
- 未返回列名数组、且 `resultRows` 为对象时的保守重排：
  - 识别 `select * from <table>`（单表、无 join/where/order/limit/union/with 等关键字），按本地缓存的 `columns.sort_no` 顺序重建列清单并重排 JsonObject 值；表名含 schema 时仅取对象名部分。
  - 识别显式列列表：截取 `select` 与第一个 `from` 之间的片段，以括号深度为 0 的逗号切分，跳过包含函数/表达式/子查询/算术符号的复杂 token。优先使用 `AS` 或尾部别名，其次 `t.col` → `col`，再按用户列序从 JsonObject 取值；缺失字段填空字符串。
  - 任意一步命中复杂结构或缓存缺失时立即回退，不做重排，保持服务端顺序。
- `resultRows` 为数组时不调整原始顺序，仅在存在列名数组/缓存时提供列头，兼容旧格式。
- 本地列缓存来源于 `information_schema.columns` 的 `ordinal_position`，默认 schema `leshan`，缓存缺失不会抛异常，只回退到后端顺序。

## SQL 子窗口布局说明
- 每个编辑 Tab 固定使用上下分割的 `JSplitPane`：上侧为编辑器工具条+正文，下侧为结果集/消息导航区，不再存在“编辑/结果”切换标签。
- 默认分割比例约 65%（编辑）/35%（结果），工具条提供 70/30、60/40、40/60 预设，拖拽分隔条也会实时记忆位置。
- 执行 SQL 后结果直接出现在下方，编辑器始终保持可见，可继续修改/再次执行。

### 分屏视图与最大化逻辑
- “结果最大化/编辑器最大化”通过隐藏另一侧组件并将分隔条移动至 1.0/0.0，同时保留上次分割位置；再次点击会调用 `restoreSplitLayout` 恢复双栏并还原分隔条。
- 分隔条尺寸保持 8px，组件从不从容器移除，避免因 `setCenter(null)` 或重复 add 导致的空白；任意异常布局下可点击预设比例或最大化按钮恢复。
- 常见排查：确认两侧组件的 visible 状态、`lastDividerLocation` 记录以及分隔条位置，必要时重置预设即可恢复界面。

### 配置与排错
- 相关配置集中在 `AsyncSqlConfig`，包含 BASE_URL、Token、AES Key/IV 等；证书忽略逻辑在 `TrustAllHttpClient`。
- 元数据或查询列顺序异常时，可开启操作日志观察“执行元数据 SQL”与任务完成日志，确认返回的列名顺序；必要时清空本地 SQLite（工具-重置元数据）后重试。

## 日志与元数据修复补充说明

### HTTPS 全量日志输出
- 网络层统一在 `SqlExecutionService#postJson` 中拦截，向右侧“操作日志”打印完整请求/响应：URL、方法、请求头、请求体 JSON、HTTP 状态码、响应头与响应体。
- 若响应体超过 200,000 字符，会写入 `%USERPROFILE%\\.Sql_client_v3\\logs\\http-YYYYMMDD.log` 并在日志面板提示文件路径，同时仅展示前 200,000 字符以防 UI 卡顿。
- 元数据刷新、SQL submit/status/result/cancel/list 均复用此日志通道，便于排障。

### 元数据聚合 payload 与差分策略
- 固定 schema/dbUser 均为 `leshan`，避免因 Tab 选择的 dbUser 导致元数据不一致。
- 采用“单行聚合” SQL 拉取：
  - 表：`string_agg(table_name,'|')`；视图：`string_agg(table_name,'|')`；函数/过程：聚合 `pg_proc.proname`；字段：以 `obj\tcol1,col2` 聚合、对象间以换行分隔。
- 客户端解析 payload 后写入本地 SQLite，并新增 `meta_snapshot` 记录各类型的 SHA-256 哈希：
  - 表/视图/函数/过程：按哈希差异计算新增/删除，对应插入/删除 `objects`。
  - 字段：以对象为粒度比较列顺序变更，必要时重建该对象的全部列记录。
- `meta_snapshot` 未命中视为首次全量；命中且哈希一致则跳过对应类型，避免重复写入。

### 启动全量同步
- 主窗口完成初始化后即触发异步元数据刷新，`meta_snapshot` 为空时执行全量同步；后续刷新仅更新变动的类别/对象。
- 清空本地缓存（菜单“工具-重置元数据”）会删除 `objects/columns/meta_snapshot`，强制下一次重新全量拉取。

### 操作日志降噪
- 笔记链接同步的成功日志已降级为 DEBUG，不再通过右侧“操作日志”面板输出“更新链接/Update link”等文案；如需排查可改用文件日志（`NoteRepository`）。
- 仍保留解析/保存失败的提示，且改为“保存笔记关联失败”以便快速定位异常。

### 本地 SQLite 存储路径与迁移
- 本地缓存数据库统一存放在 `%USERPROFILE%\\.Sql_client_v3\\metadata.db`（Windows 为 `C:\\Users\\<用户名>\\.Sql_client_v3\\metadata.db`）。
- 启动时若目录不存在会自动创建；若发现旧的工作目录下仍有 `metadata.db` 且新目录为空，会自动复制至新位置并在日志中提示已迁移。
- 若新旧文件同时存在，优先使用用户目录版本，不会自动删除旧文件；可手动检查/清理旧的 `metadata.db`。
- 常见问题排查：
  - 权限不足/路径中包含空格时请确认有写权限，并使用绝对路径启动应用；
  - 如遇“无法创建本地数据库目录”异常，可手动创建 `%USERPROFILE%\\.Sql_client_v3` 后重试；
  - 迁移失败会在日志中提示“迁移本地数据库失败”，可手动复制旧文件至新目录再启动。

### 异步接口对接（2024-05 更新）

#### 固定请求头与安全要求
- 所有 HTTPS POST 请求都使用 `Content-Type: application/json;charset=UTF-8`。
- 统一附带 `X-Request-Token: WAF_STATIC_TOKEN_202405`，为测试环境固定值，不再支持可变 token。

#### 接口总览
- **/api/jobs/submit**：`{"encryptedSql": "<Base64 AES>", "maxResultRows":5000?, "dbUser":"leshan_app"?, "label":"xxx"?}`。`dbUser` 逻辑库用户默认 `leshan`，`maxResultRows` 仅影响首次内存快照，并不限制最终可见行数。
- **/api/jobs/status**：`{"jobId":"..."}`，返回 `status/progressPercent/elapsedMillis` 等任务信息。
- **/api/jobs/result**：分页拉取 `{"jobId":"...", "removeAfterFetch":true?, "page":1?, "pageSize":200?}`。响应字段包含 `success/status/progressPercent/resultRows/columns/returnedRowCount/actualRowCount/maxVisibleRows/maxTotalRows/hasNext/truncated/note/page/pageSize`，客户端全部做空值兼容。

#### 分页策略与上限
- 默认 `page=1`、`pageSize=200`，客户端与 README 都建议显式传入。`config.properties` 提供 `result.pageSize` 默认 200，`result.pageSize.max` 固定上限 1000。
- 前端会在 Tab 工具栏读取用户输入的 pageSize；`<1` 直接回退默认值，`>1000` 自动裁剪为 1000 并在操作日志输出“已裁剪到 1000”。
- `hasNext`、`returnedRowCount`、`actualRowCount`、`maxVisibleRows`、`maxTotalRows`、`truncated`、`note` 全部写入 OperationLog 以便排障。`hasNext` 的计算基于 1000 行可见窗口，无法通过翻页突破上限。
- 常规 SQL 默认仅抓取第一页并根据 `allowPagingAfterFirstFetch` 决定是否清理缓存；元数据同步仍使用 `executeSyncAllPages` 自动翻页，但单页上限依然受 1000 限制。

#### dbUser 选项（每个 Tab 独立）
- 每个 SQL 编辑 Tab 的工具栏新增 `dbUser` 下拉框，可在 `leshan / leshan_app` 间切换，默认 `leshan`。选择仅作用于当前 Tab 的 `/jobs/submit` 请求，互不串台。
- Tab 同时提供 `pageSize` 输入框（预置 200/500/1000，可自定义），提交时用于 `/jobs/result` 的分页大小，独立于其他 Tab。

#### 常见问题与提示
- **pageSize 被裁剪**：日志出现 “pageSize=3000 超出上限，已裁剪到 1000” 时，实际请求体为 1000，符合后端硬上限。
- **结果被截断**：响应 `truncated=true` 或 `note` 提示“maxVisibleRows=1000”时，客户端会在日志提示且无法再通过翻页取更多数据。
- **TTL 过期**：`success=false` 且消息为空时，前端统一提示“结果已过期，请重新执行 SQL”，不会导致崩溃。
