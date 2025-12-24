# SQL Notebook 用户操作指引

面向 Windows 11 风格的多标签 SQL 笔记本，基于 Java 22 + Swing + RSyntaxTextArea，支持 PostgreSQL / Hive 的编辑、执行与本地元数据缓存。目标数据库版本固定：PostgreSQL 12.7 on x86_64-pc-linux-gnu（gcc 4.8.5）。

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

## 问题根因与修复说明
- **失败链路**：元数据刷新在拉取表/视图/字段时直接调用 `/waf/api/jobs/result`，SQL 只做 `LIMIT 1000` 的 keyset 分页，未在客户端裁剪 pageSize 或使用 SQL 级 OFFSET 分页；当 schema 超过 1000 行时，后端窗口被截断并返回 `RESULT_NOT_READY` / HTTP 410 `RESULT_EXPIRED`，旧逻辑在 60s 超时时抛出异常，刷新失败且 staging 未写回。代码可见 `MetadataRefreshService#waitUntilReady` 的轮询与超时逻辑。
- **直接原因**：客户端缺少与服务端 README 对齐的 1000 行硬上限裁剪与 SQL 级分页，也未对 `RESULT_NOT_READY/RESULT_EXPIRED` 做语义化分支，导致 `/jobs/result` 在未归档或过期时直接失败。
- **修复方案**：新增 `tools.sqlclient.remote.RemoteSqlClient` 统一封装 submit/status/result/list/cancel，自动注入 Token + AES/CBC，加上 pageSize/limit≤1000 裁剪与 401/410 友好提示；`MetadataRefreshService#fetchPagedObjects` 与 `#fetchColumns` 改用 `ORDER BY + OFFSET/LIMIT` SQL 分页循环，`waitUntilReady` 区分 `RESULT_NOT_READY/RESULT_EXPIRED`，取消时调用 `/jobs/cancel` 并保持本地缓存不被覆盖。

## 元数据刷新（3 万条规模）
- **窗口硬约束**：后端单页最多 1000 行，客户端在调用 `/jobs/result` 时强制裁剪 `pageSize/limit` 到 1000，并在日志中提示被裁剪的请求参数，避免误解为可以绕过上限。
- **SQL 级分页**：所有对象与字段都使用 `ORDER BY + OFFSET/LIMIT` 分页，LIMIT 恒定 1000（PG12.7 兼容），直到某页返回行数 < 1000 为止。实现集中在 `MetadataRefreshService#fetchPagedObjects/fetchColumns`，按 schema、对象名、字段顺序排序，保证页内稳定性。
- **状态机与容错**：单批次最长等待 60s，遇到 `RESULT_NOT_READY` 继续轮询，HTTP 410/`RESULT_EXPIRED` 会自动重提一次，`overloaded=true` 采用 500ms 起步的指数退避（最高 8s，最多 3 次）。归档报错会立即失败并回滚 staging。
- **取消与清理**：工具菜单新增“取消元数据刷新”，会设置本地取消标记并对正在运行的 job 调用 `/jobs/cancel`（reason=`Cancelled by UI`），取消后不覆盖本地缓存。
- **本地缓存与 UI 性能**：批次结果先写入 SQLite 分区表 `objects_staging/columns_staging`，成功完成后一次性事务替换正式表；刷新、列抓取与日志输出均在后台线程执行，UI 仅接收进度文本，不会一次性把 3 万行塞进 JTable。
- **开发者自测入口**：工具菜单的 “Refresh Metadata (Large)” 入口会执行完整刷新并弹窗展示 `tablesCount/columnsCount/batches/duration`，用于验证大规模情况下不会超时或卡死。

## 异步 SQL 执行前端适配说明
### 架构与数据流
- 客户端不再直接调用旧的同步 SQL 接口，而是针对 `/waf/api/jobs/*` 异步接口：编辑器 SQL 文本 → AES/CBC/PKCS5Padding 加密 → `/jobs/submit` 提交返回 jobId → 后台线程轮询 `/jobs/status` → 完成后 `/jobs/result` 拉取结果集 → `QueryResultPanel` 展示。
- 所有请求统一附带 `X-Request-Token: WAF_STATIC_TOKEN_202405`，并使用本地信任所有证书的 HttpClient 发送。

### 核心类职责
- `tools.sqlclient.exec.SqlExecutionService`：封装提交、轮询与结果获取；日志中输出 jobId、进度与耗时。
- `tools.sqlclient.remote.RemoteSqlClient`：统一封装 submit/status/result/list/cancel 的 HTTPS POST 调用，自动注入 Token/Header、AES/CBC/PKCS5Padding 加密，并处理 401/410/RESULT_NOT_READY 分支。
- `tools.sqlclient.remote.RemoteSqlConfig`：默认的 BASE_URL、Token、AES Key/IV 配置与 URL 构造工具。
- `tools.sqlclient.remote.AesCbc`：负责将 SQL 明文加密/解密成 Base64 AES 密文，供 `/jobs/submit` 使用，并配有回环单测。
- `tools.sqlclient.exec.AsyncJobStatus`：表示后台任务的状态、进度与摘要。
- `tools.sqlclient.ui.QueryResultPanel`：支持进度条、状态/耗时展示，并在任务完成后渲染结果集或错误信息。
- `tools.sqlclient.ui.AsyncJobListDialog`：展示当前后台任务列表，提供刷新与取消入口。

### UI 操作
- Ctrl+Enter 依旧执行当前 SQL 块；提交后状态栏与结果面板先显示“已提交，等待执行”，随后轮询进度并更新百分比、耗时与 jobId。
- 任务完成时，结果面板自动填充 SELECT 结果或 DDL/DML 影响行数提示；失败/取消则在同一面板展示错误信息。
- 通过菜单“视图 -> 异步任务列表”打开任务窗口，可查看 ACCEPTED/QUEUED/RUNNING/CANCELLING 中的任务并手动取消。
- 工具栏的“停止”按钮会向后端发送 `/jobs/cancel` 请求，状态栏提示“已发送取消请求”，最终状态由后续轮询决定。

### 配置覆盖
- 默认 BASE_URL/Token/AES Key/IV 定义在 `RemoteSqlConfig` 中，可按环境需要改写；如已有外部配置中心，可在初始化时覆盖这些常量（也可通过 `-DASYNC_SQL_BASE_URL` 覆盖基础域名）。
- 仍复用已有的 WAF 域名/证书忽略策略，HttpClient 创建逻辑保持不变。

### 调试与常见问题
- 操作日志（OperationLog）会输出 jobId、提交/状态轮询/完成时的进度记录，便于排查长任务。
- 若响应 `success:false` 或 HTTP 非 200，客户端会在结果面板展示错误提示且不中断 UI。
- Token 错误或网络异常时，日志与状态栏会提示失败原因；SQL 本身失败会在结果面板呈现后端返回的 errorMessage。

## 前端开发技术文档：元数据获取与列顺序

### SqlExecResult 构建规范与非查询结果展示
- 统一通过 `SqlExecResult.builder(sql)` 构建结果对象，业务代码禁止直接 `new SqlExecResult(...)`。旧构造器仅用于类内部或兼容调用，字段新增/删减时只需调整 Builder 默认值。
- Builder 默认提供空列表与安全兜底：columns/rows/rowMaps/notices/warnings 为空时自动替换为 `List.of()`，`rowsCount` 默认为行数或 rowMaps 大小，`success` 默认为 `true`，`hasResultSet` 自动从列/行是否为空推断（可显式覆盖）。
- 查询结果：显式设置 `hasResultSet=true`，并填充 columns/rows/rowMaps，确保行列长度一致。
- 非查询结果（DDL/DML/无结果集）：`hasResultSet=false`，columns/rows/rowMaps 置空，同时填充 `rowsAffected/updateCount/commandTag` 中至少一项，以便 UI 显示影响行数。
- 影响行数展示优先级：`rowsAffected` 优先，其次 `updateCount`，再次尝试从 `commandTag` 解析（如 `INSERT 0 3` => 3）。缺失时显示为空，不再因 null 传入 primitive 触发编译或拆箱异常。

### 元数据同步（本地优先 + 分批差分）
- **本地优先**：对象树、模糊联想与列提示全部只读本地 SQLite，常规操作不触发任何远端调用。
- **全量同步仅两处**：程序启动后异步拉取一次表/视图/函数/过程名单；工具栏“刷新元数据”会再次按相同逻辑执行，全程不触碰字段。
- **SQL 分页**：所有清单统一使用 `ORDER BY name OFFSET :offset LIMIT 1000`，批量 SQL 自带 `LIMIT 1000`，按页递增直到返回不足 1000 行，不依赖 `/jobs/result` 的 `hasNext`。
- **清单 SQL 模板**：
  - 表：`SELECT table_schema AS schema_name, table_name AS object_name FROM information_schema.tables WHERE table_schema='leshan' AND table_type='BASE TABLE' ORDER BY table_name OFFSET :offset LIMIT 1000`。
  - 视图：`SELECT table_schema AS schema_name, table_name AS object_name FROM information_schema.views WHERE table_schema='leshan' ORDER BY table_name OFFSET :offset LIMIT 1000`。
  - 函数：`SELECT n.nspname AS schema_name, p.proname AS object_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='leshan' AND (p.prokind='f' OR p.proisagg=FALSE) ORDER BY p.proname OFFSET :offset LIMIT 1000`。
  - 过程：`SELECT n.nspname AS schema_name, p.proname AS object_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='leshan' AND p.prokind='p' ORDER BY p.proname OFFSET :offset LIMIT 1000`（`prokind` 不可用时返回空列表以避免大包）。
- **差分写库**：全量同步只对新增/删除对象做 INSERT/DELETE，移除的表/视图会顺带清理对应列缓存，避免全表重建带来的抖动。

### 批次拉取与分页限制
- 后端 `/jobs/result` 单页可见行数上限 1000，不再依赖 `hasNext`，每批 SQL 自行限制 1000 行并按 `OFFSET = page*1000` 循环，直到返回行数不足 1000。
- 请求头、Token 与 HTTPS POST 仍复用异步执行链路，但默认只在启动/手动刷新/DDL 后的局部校验中触发。

### 字段按需与联想
- 列信息仅在需要时抓取：联想命中某表、或用户显式触发“刷新列”时，才对单表执行 `SELECT column_name, ordinal_position ... ORDER BY ordinal_position` 并落库为 `sort_no`。
- ALTER TABLE 成功后会强制刷新该表列缓存，其它 DDL 不会触碰字段；CREATE TABLE/VIEW 会新增对象记录但列仍按需加载。
- 列缓存缺失时自动触发单表请求，返回后按 `ordinal_position` 排序，联想/结果渲染直接复用本地顺序。

### DDL 变更的局部刷新
- 执行成功后的 SQL 会做轻量正则解析：识别 CREATE/DROP/ALTER + TABLE/VIEW/FUNCTION/PROCEDURE，并提取 `schema.object` 或裸对象名（默认 schema=`leshan`）。
- CREATE/ALTER：只针对命中的对象向远端校验存在性并写入/更新本地对象表；ALTER TABLE 额外刷新该表列缓存。
- DROP：立即删除本地对象与列缓存，不触发全量同步，避免无关对象被误删。
- 未能解析出对象名时不做远端请求，保持“按需才上网”的原则。

### 结果栏显示修复
- 结果面板在重用 pending 面板时未必会重新应用渲染器，部分环境下 JTable 不会自动刷新背景与结构，易出现“已完成但表格空白”的错觉。
- 现在每次渲染/报错都会重新绑定条纹渲染器、调用 `revalidate/repaint` 并重算列宽，确保列头/行数据即时出现在结果栏。
- 结果计数、状态与 message 仍与后端字段同步；如仍为空白，可通过操作日志确认后端是否返回 `returnedRowCount`/`resultRows`。

## SQL 子窗口布局说明
- 每个编辑 Tab 固定使用上下分割的 `JSplitPane`：上侧为编辑器工具条+正文，下侧为结果集/消息导航区，不再存在“编辑/结果”切换标签。
- 默认分割比例约 65%（编辑）/35%（结果），工具条提供 70/30、60/40、40/60 预设，拖拽分隔条也会实时记忆位置。
- 执行 SQL 后结果直接出现在下方，编辑器始终保持可见，可继续修改/再次执行。

### 分屏视图与最大化逻辑
- “结果最大化/编辑器最大化”通过隐藏另一侧组件并将分隔条移动至 1.0/0.0，同时保留上次分割位置；再次点击会调用 `restoreSplitLayout` 恢复双栏并还原分隔条。
- 分隔条尺寸保持 8px，组件从不从容器移除，避免因 `setCenter(null)` 或重复 add 导致的空白；任意异常布局下可点击预设比例或最大化按钮恢复。
- 常见排查：确认两侧组件的 visible 状态、`lastDividerLocation` 记录以及分隔条位置，必要时重置预设即可恢复界面。

### 配置与排错
- 相关配置集中在 `RemoteSqlConfig`，包含 BASE_URL、Token、AES Key/IV 等；证书忽略逻辑在 `TrustAllHttpClient`。
- 元数据或查询列顺序异常时，可开启操作日志观察“执行元数据 SQL”与任务完成日志，确认返回的列名顺序；必要时清空本地 SQLite（工具-重置元数据）后重试。

### 常见排查（列顺序 / 空列头 / pageSize 裁剪）
- 列乱序：查看日志中的 `columns=` 与 `first row keys=`，确认是否触发了“正在拉取表 <name> 的字段顺序”提示；单表 `SELECT *` 会等待本地元数据顺序并在拉取完成后刷新 UI。
- 空列头：结果 0 行时也应看到列名；若未出现，确认后端是否返回了列名数组，或检查日志中的 SELECT 解析结果与兜底 `col_n` 提示。
- pageSize 被裁剪：日志会输出 “pageSize=5000 超出上限，已裁剪到 1000”等信息；确认工具栏“默认返回条数”与每个 Tab 的 pageSize 输入框均在 1~1000 范围内。

## 日志与元数据修复补充说明

### HTTPS 全量日志输出
- 网络层统一在 `RemoteSqlClient` 中拦截，向右侧“操作日志”打印完整请求/响应：URL、方法、请求头、请求体 JSON、HTTP 状态码、响应头与响应体。
- 若响应体超过 200,000 字符，会写入 `%USERPROFILE%\\.Sql_client_v3\\logs\\http-YYYYMMDD.log` 并在日志面板提示文件路径，同时仅展示前 200,000 字符以防 UI 卡顿。
- 元数据刷新、SQL submit/status/result/cancel/list 均复用此日志通道，便于排障。

### 元数据分页同步与差分策略
- 固定 schema/dbUser 均为 `leshan`，对象树不再依赖用户选择的 dbUser，避免串台。
- **SQL 分页 1000 行**：对象清单按 `ORDER BY name OFFSET :offset LIMIT 1000` 循环，避免超过服务端硬上限时被截断；分页变量只由客户端控制。
- **差分策略**：仅增量插入/删除对象记录，删除表/视图时同步清理列缓存；字段始终按需拉取，不再全库级扫描。
- **字段顺序保证**：单表列请求固定按 `ordinal_position` 排序并落库为 `sort_no`，UI 直接按该顺序展示，缺失缓存时回退到后端顺序。
- **可追溯调试**：每批请求的 SQL 与返回前 200 行会写入 `%USERPROFILE%\.Sql_client_v3\logs\metadata-*.log`，OperationLog 仅输出批次计数和耗时。

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

#### 异步 SQL 结果一致性窗口与客户端重试策略
- 状态 `SUCCEEDED` 但结果暂不可用时（`success=false` 且 message 含 “Result expired/not available” 或返回行/计数矛盾），前端自动进入自愈重试，不再直接宣告失败。
- 可配置项（System Property 或 `config.properties` 均可）：
  - `ASYNC_SQL_RESULT_RETRY_MAX`：默认 `8`，最多重试次数（含首次请求）。
  - `ASYNC_SQL_RESULT_RETRY_BASE_DELAY_MS`：默认 `150`，首次重试等待时间（毫秒）。
  - `ASYNC_SQL_RESULT_RETRY_MAX_DELAY_MS`：默认 `1000`，重试延迟上限，采用递增等待避免 UI 卡顿。
- 日志会输出 `jobId/attempt/page/base/delay/returnedRowCount/actualRowCount/hasResultSet/message`，便于定位“已完成但结果短暂不可用”的窗口期。
- SUCCEEDED 后拉取结果在后台线程执行，即便重试也不会阻塞 UI；仅在拿到非空结果后才进入本地 SQLite 写库。部分批次失败不会影响已成功写入的批次。

#### 低延迟调度与资源治理字段
- 后端新增 `queuedAt/queueDelayMillis/overloaded/threadPool(poolSize/activeCount/queueSize/taskCount/completedTaskCount/jobStoreSize)`，前端在 submit/status/result 日志中透传，便于观察排队与池状态。
- `/jobs/submit` 直接 `success:false` 或 `overloaded=true` 视为终态拒绝，不进入轮询；`/jobs/status` 返回 `FAILED` 且 message 含 queue delay exceeded 亦为终态，UI 直接提示“排队超时/系统繁忙”。

#### 分页 page 基准与 AUTO 探测规则
- 默认仍按 1-based 请求 `page=1`，当发现 `hasResultSet=true && actualRowCount>0` 但 `returnedRowCount=0` 或返回空行时，会自动切换到另一种 page 基准（0-based）再试一次，并在当前刷新流程内缓存成功的基准。
- `ASYNC_SQL_RESULT_PAGE_BASE`：`AUTO`（默认，先 1-based 再按需回退）、`0`、`1`。通过 System Property 或 `config.properties` 覆盖。

#### removeAfterFetch 策略选择
- 元数据/短结果优先保证可重试性，首次拉取统一使用 `removeAfterFetch=false`。默认 `AUTO` 不会做二次清理，避免清理后无法重试；如确需主动清理，可设置为 `TRUE`。
- `ASYNC_SQL_RESULT_FETCH_REMOVE_AFTER`：`AUTO`（默认，安全保留缓存）、`TRUE`（解析成功后追加一次 `removeAfterFetch=true`）、`FALSE`（永不清理）。
- 日志会显示 `removeAfterFetch=<true|false>` 以便确认策略，若后端专门提供清理接口可按需开启。

#### 结果重试与自动重提
- 结果请求的重试仍受上述退避参数控制，矛盾检测（status 已 SUCCEEDED 且 actualRowCount > 0 但返回 0 行）会自动切换 page 基准并视为暂不可用。
- 元数据批处理遇到“Result expired / not available”或返回空行的暂不可用场景，会在一次重试失败后自动重提任务（单批仅 1 次），日志包含 `resubmitted=true`、原/新 jobId。
- 非元数据查询保持谨慎，不会自动重提；如超出重试上限需用户重新执行。

#### 排队节奏与上限字段
- `maxVisibleRows=1000`、`pageSize` 上限 1000（遵循后端固定窗口），前端默认 pageSize 200，超出会自动裁剪并记录日志。
- 同步接口的 `maxTotalRows/capped` 字段在结果日志中透传，便于确认是否触发行数封顶。

#### /jobs/result 响应与列推断
- `resultRows` 依旧兼容 `resultRows/rows/data` 三个字段，先拿数组再落表。
- **列顺序决策链**：
  1. 如果是 `SELECT * FROM <单表>`，优先按本地缓存的字段顺序（metadata.columns.sort_no）渲染；若缓存缺失会后台拉取后刷新；
  2. 显式列清单（`SELECT a AS x, b ...`）按用户书写顺序+别名展示，与后端返回顺序无关；
  3. 复杂查询（JOIN/子查询/函数返回）优先使用后端返回的 `columns/resultColumns/columnNames` 顺序；
  4. 后端无列名时会对 SELECT 与 FROM 之间的顶层逗号分隔投影做轻量解析，提取别名/标识符；解析失败则稳定生成 `col_1..n` 占位。
- **行值重排**：渲染前显式根据决策出的最终列顺序重排行值；即便 `resultRows` 为空，也会按上述策略生成表头，避免“0 行时无列名”。
- OperationLog 额外打印 `jobId/returnedRowCount/resultRows.size/columns.size/第一行 keys` 等调试信息，解析异常会在 UI 直接提示错误。

### 结果列顺序与空结果展示
- `SELECT * FROM <单表>`：按照本地元数据的 `ordinal_position` 展示列顺序；若缓存缺失会触发异步拉取，加载完成后自动刷新对应结果面板。
- `SELECT colA AS x, colB ...`：严格遵循用户输入的投影顺序与别名，别名优先作为列头。
- 复杂查询（JOIN/聚合/函数返回）：若后端提供列名数组则按返回顺序展示；否则按轻量解析得到的投影列表稳定落表。
- 空结果集仍会显示列头，优先级：后端列名数组 > 显式 SELECT 解析 > 单表 `*` 使用本地列缓存 > 兜底 `col_1..n`。日志会提示任何裁剪/兜底行为。

#### 列名/列值错位修复（2024-07）
- 根因：当后端已返回列名时，轻量解析器仍可能用 `SELECT` 投影覆盖真实列（例如 `a.*`、`a.lx_flag, *`、`acct_id::varchar`），导致列头数量与行值不一致；同时重复列名会被去重。
- 修复策略：
  - 只要后端列名非空，默认直接采用其顺序与数量；仅在纯粹 `SELECT * FROM <单表>` 且元数据顺序已缓存时，才用元数据重排；其它情况不再用轻量解析结果覆盖。
  - 轻量解析仅在后端列名缺失时兜底生成列头，避免 `a.*` 与 `::` 转换被误判。
- 重复列名展示规则：不再去重或裁剪列数，JTable 直接展示重复列头；必要时可在 UI 层追加序号，但默认保持与行值一一对应。

#### removeAfterFetch 处理
- 首次 `/jobs/result` 统一使用 `removeAfterFetch=false`，避免在可重试窗口内提前清空缓存；当配置为 `AUTO/TRUE` 且结果已成功解析后，会在后台追加一次 `removeAfterFetch=true` 清理请求以释放服务端缓存。
- 若配置为 `FALSE`，则完全跳过清理请求，确保即便后端在 `removeAfterFetch=true` 后不允许重试也不会影响可用性。

#### 分页策略与上限
- 默认 `page=1`、`pageSize=200`，客户端与 README 都建议显式传入。`config.properties` 提供 `result.pageSize` 默认 200，`result.pageSize.max` 固定上限 1000。
- 前端会在 Tab 工具栏读取用户输入的 pageSize；`<1` 直接回退默认值，`>1000` 自动裁剪为 1000 并在操作日志输出“已裁剪到 1000”。
- `hasNext`、`returnedRowCount`、`actualRowCount`、`maxVisibleRows`、`maxTotalRows`、`truncated`、`note` 全部写入 OperationLog 以便排障。`hasNext` 的计算基于 1000 行可见窗口，无法通过翻页突破上限。
- 常规 SQL 默认仅抓取第一页并根据 `allowPagingAfterFirstFetch` 决定是否清理缓存；元数据同步仍使用 `executeSyncAllPages` 自动翻页，但单页上限依然受 1000 限制。
- 工具栏新增“默认返回条数”输入框 + “应用”按钮（1~1000，步长 10），修改后立即作用于后续查询；会持久化到 `app_state.default_page_size` 并通过 `Config.overrideDefaultPageSize` 影响 `/jobs/result` 的 pageSize 与 `/jobs/submit` 的 maxResultRows。超过 1000 会被裁剪到 1000，0/负数会回退到安全默认值 200，日志均有提示。

#### dbUser 选项（每个 Tab 独立）
- 每个 SQL 编辑 Tab 的工具栏新增 `dbUser` 下拉框，可在 `leshan / leshan_app` 间切换，默认 `leshan`。选择仅作用于当前 Tab 的 `/jobs/submit` 请求，互不串台。
- Tab 同时提供 `pageSize` 输入框（预置 200/500/1000，可自定义），提交时用于 `/jobs/result` 的分页大小，独立于其他 Tab。

#### 常见问题与提示
- **pageSize 被裁剪**：日志出现 “pageSize=3000 超出上限，已裁剪到 1000” 时，实际请求体为 1000，符合后端硬上限。
- **结果被截断**：响应 `truncated=true` 或 `note` 提示“maxVisibleRows=1000”时，客户端会在日志提示且无法再通过翻页取更多数据。
- **TTL 过期**：`success=false` 且消息为空时，前端统一提示“结果已过期，请重新执行 SQL”，不会导致崩溃。

## 前端开发技术文档（sql_client_v3）

### 协议与配置
- BASE_URL：默认 `https://leshan.paas.sc.ctc.com/waf/api`（`RemoteSqlConfig.BASE_URL`）。可通过 `-DASYNC_SQL_BASE_URL` 覆盖。
- 必需 Header：`Content-Type=application/json;charset=UTF-8`、`X-Request-Token: WAF_STATIC_TOKEN_202405`。
- AES：算法 `AES/CBC/PKCS5Padding`，Key=`LeshanAESKey1234`，IV=`LeshanAESIv12345`，UTF-8 编码，Base64 传输（`RemoteSqlConfig`/`AesCbc`，单测 `AesCbcTest` 覆盖加解密回环）。
- 所有接口一律 POST，SSL 仍沿用信任所有证书的 HttpClient。

### 接口字段速查
- `/jobs/submit`：请求 `encryptedSql`（必填，AES+Base64 后的 SQL）、`maxResultRows?`、`dbUser?`（默认 `leshan`，白名单 `leshan`/`leshan_app`）、`label?`；响应包含 `jobId/status/progressPercent/queuedAt/queueDelayMillis/overloaded/message/threadPool`。
- `/jobs/status`：请求 `jobId`；响应同上，用于轮询进度。
- `/jobs/result`：请求 `jobId` + `removeAfterFetch?` + **分页**（二选一：`page/pageSize` 或 `offset/limit`，客户端会把 `pageSize/limit` 裁剪到 ≤1000 并在 `note`/日志提示）；响应 `success/status/code (RESULT_NOT_READY|RESULT_EXPIRED)/resultAvailable/resultRows/columns/returnedRowCount/actualRowCount/maxVisibleRows/maxTotalRows/hasNext/truncated/note/threadPool/message`。
- `/jobs/list`：请求体 `{}`，响应 `jobs:[<status 对象>]`。
- `/jobs/cancel`：请求 `jobId`、`reason?`；响应同 status。

### 元数据刷新实现（PG 12.7）
- SQL 级分页：对象与字段均按 `ORDER BY schema/object/ordinal_position OFFSET :offset LIMIT 1000` 逐页提交，见 `MetadataRefreshService#fetchPagedObjects` 与 `#fetchColumns`。
- 轮询与重试：`waitUntilReady` 使用 `/jobs/result` 轮询，`RESULT_NOT_READY` 继续等待，HTTP 410/`RESULT_EXPIRED` 重提一次；单批超时 60s；overload 采用 500ms 起步指数退避。
- 取消：`MetadataService.cancelRefresh` 设置取消标记并调用 `/jobs/cancel`（reason=`Cancelled by UI`），取消后 staging 不会覆盖正式表。
- 本地安全：结果先写入 `objects_staging/columns_staging`，成功后一次性 `swapFromStaging`；失败/取消保留旧缓存。

### 常见错误与排障
- 401：Token 错误或缺失，`RemoteSqlClient` 会抛出 “HTTP 401 未授权” 并在 OperationLog 打印请求/响应；请确认 `X-Request-Token`。
- 410/`RESULT_EXPIRED`：结果已过期，UI 提示“结果已过期”，元数据刷新会跳过覆盖，可重新执行提交。
- `RESULT_NOT_READY`：表示结果仍在落盘，客户端会继续轮询；日志会显示 attempt/page 信息。
- AES 解密失败：检查 Key/IV 与 UTF-8，参考 `AesCbcTest` 或下方 PowerShell 脚本生成密文。
- pageSize/limit >1000：客户端自动裁剪并在 `ResultResponse.note` 与日志中提示裁剪值。

### PowerShell 最小验证清单（HTTPS POST）
```powershell
# 1) 准备 AES 密文（保持 UTF-8）
$plainSql = "SELECT 1 as demo"
$key = [Text.Encoding]::UTF8.GetBytes("LeshanAESKey1234")
$iv = [Text.Encoding]::UTF8.GetBytes("LeshanAESIv12345")
$aes = [System.Security.Cryptography.Aes]::Create()
$aes.Mode = "CBC"; $aes.Padding = "PKCS7"; $aes.Key = $key; $aes.IV = $iv
$encryptor = $aes.CreateEncryptor()
$input = [Text.Encoding]::UTF8.GetBytes($plainSql)
$cipherBytes = $encryptor.TransformFinalBlock($input,0,$input.Length)
$encryptedSql = [Convert]::ToBase64String($cipherBytes)

$baseUrl = "https://leshan.paas.sc.ctc.com/waf/api"
$headers = @{ "X-Request-Token" = "WAF_STATIC_TOKEN_202405"; "Content-Type"="application/json;charset=UTF-8" }

# 2) submit
$submitBody = @{ encryptedSql = $encryptedSql; dbUser = "leshan"; label = "ps-test" } | ConvertTo-Json
$submit = Invoke-RestMethod -Method Post -Uri "$baseUrl/jobs/submit" -Headers $headers -Body $submitBody
$jobId = $submit.jobId

# 3) status
$statusBody = @{ jobId = $jobId } | ConvertTo-Json
$status = Invoke-RestMethod -Method Post -Uri "$baseUrl/jobs/status" -Headers $headers -Body $statusBody

# 4) result（pageSize 不要超过 1000）
$resultBody = @{ jobId = $jobId; page = 1; pageSize = 200; removeAfterFetch = $true } | ConvertTo-Json
$result = Invoke-RestMethod -Method Post -Uri "$baseUrl/jobs/result" -Headers $headers -Body $resultBody

# 5) cancel（需要时调用）
$cancelBody = @{ jobId = $jobId; reason = "Cancelled by UI" } | ConvertTo-Json
$cancel = Invoke-RestMethod -Method Post -Uri "$baseUrl/jobs/cancel" -Headers $headers -Body $cancelBody
```

## SQL 顶层类型与结果渲染规则（PostgreSQL 12.7 on x86_64-pc-linux-gnu，gcc 4.8.5）
- **顶层语句分类**：`SqlTopLevelClassifier` 逐字符跳过空白、`--`/`/* */` 注释与字符串字面量，仅读取顶层首关键字；`WITH ...` 会定位到主语句。
  - SELECT/VALUES/SHOW/EXPLAIN（含 WITH 主语句为这些）视为 **ResultSet**。
  - 其他顶层关键字（INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/GRANT 等）统一视为 **非查询**。
- **ResultSet 渲染**：仅当后端标记 `hasResultSet/isSelect/resultType` 为 true，或分类器判定为 ResultSet 时才进入列适配/投影解析；列定义数量与每行数据列数必须一致，否则跳过重排以避免“多列表头 + 空白行”。
- **非查询渲染**：顶层非查询或 `hasResultSet=false` 时走消息视图，显示状态、commandTag、affectedRows、elapsed/jobId 等，不做任何列解析/星号展开。CommandTag 与 affectedRows 会直接展示，无法判定时标记为 N/A。
- **影响行数获取策略**：优先使用后端 `updateCount/affectedRows(rowsAffected)` 且值≥0；否则从 commandTag 匹配 `^(INSERT|UPDATE|DELETE|SELECT)\s+\d+` 补齐；仍不可得时显示 N/A，但 commandTag 依然保留。
