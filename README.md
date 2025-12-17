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

### 元数据同步（本地优先 + 分批差分）
- **本地优先**：对象树、模糊联想与列提示全部只读本地 SQLite，常规操作不触发任何远端调用。
- **全量同步仅两处**：程序启动后异步拉取一次表/视图/函数/过程名单；工具栏“刷新元数据”会再次按相同逻辑执行，全程不触碰字段。
- **Keyset 分批**：所有清单统一使用 `name > :lastName ORDER BY name LIMIT 1000`，批量 SQL 自带 `LIMIT 1000`，请求 `pageSize` 固定 1000，不依赖 `/jobs/result` 的 `hasNext`。
- **清单 SQL 模板**：
  - 表：`SELECT table_name AS object_name FROM information_schema.tables WHERE table_schema='leshan' AND table_type='BASE TABLE' AND table_name > :lastName ORDER BY table_name LIMIT 1000`。
  - 视图：`SELECT table_name AS object_name FROM information_schema.views WHERE table_schema='leshan' AND table_name > :lastName ORDER BY table_name LIMIT 1000`。
  - 函数：`SELECT p.proname AS object_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='leshan' AND p.prokind='f' AND p.proname > :lastName ORDER BY p.proname LIMIT 1000`（缺少 `prokind` 时降级到 `proisagg=false`）。
  - 过程：`SELECT p.proname AS object_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='leshan' AND p.prokind='p' AND p.proname > :lastName ORDER BY p.proname LIMIT 1000`（`prokind` 不可用时返回空列表以避免大包）。
- **差分写库**：全量同步只对新增/删除对象做 INSERT/DELETE，移除的表/视图会顺带清理对应列缓存，避免全表重建带来的抖动。

### 批次拉取与分页限制
- 后端 `/jobs/result` 单页可见行数上限 1000，不再依赖 `hasNext`，每批 SQL 自行限制 1000 行并通过 `name > lastName` 循环。
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
- 相关配置集中在 `AsyncSqlConfig`，包含 BASE_URL、Token、AES Key/IV 等；证书忽略逻辑在 `TrustAllHttpClient`。
- 元数据或查询列顺序异常时，可开启操作日志观察“执行元数据 SQL”与任务完成日志，确认返回的列名顺序；必要时清空本地 SQLite（工具-重置元数据）后重试。

## 日志与元数据修复补充说明

### HTTPS 全量日志输出
- 网络层统一在 `SqlExecutionService#postJson` 中拦截，向右侧“操作日志”打印完整请求/响应：URL、方法、请求头、请求体 JSON、HTTP 状态码、响应头与响应体。
- 若响应体超过 200,000 字符，会写入 `%USERPROFILE%\\.Sql_client_v3\\logs\\http-YYYYMMDD.log` 并在日志面板提示文件路径，同时仅展示前 200,000 字符以防 UI 卡顿。
- 元数据刷新、SQL submit/status/result/cancel/list 均复用此日志通道，便于排障。

### 元数据分页同步与差分策略
- 固定 schema/dbUser 均为 `leshan`，对象树不再依赖用户选择的 dbUser，避免串台。
- **Keyset 分批 1000 行**：对象清单按 `name > lastName ORDER BY name LIMIT 1000` 循环，避免 offset/hasNext 带来的截断问题。
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

#### /jobs/result 响应与列推断
- `resultRows` 是 **对象数组**，每个元素为 `key:value` 结构；客户端优先读取该字段（缺失时兼容 `rows/data`）。
- 列名优先级：1）响应中显式的 `columns/resultColumns/columnNames`；2）遍历 `resultRows` 并集所有 key，保持“首行顺序 + 新 key 追加”稳定顺序；3）数组结果回退为 `col_1..n`。绝不再从 `SELECT *` 文本推断 `*` 列名。
- 单元格值按列顺序取 `row.opt(col)` 填充，缺失写空串；若 value 为对象/数组则序列化为紧凑 JSON，避免显示类名或空白。
- `hasResultSet=true` 但 `resultRows` 为空时表头也不会显示 `*`，保持空结果集提示即可。
- OperationLog 额外打印 `jobId/returnedRowCount/resultRows.size/columns.size/第一行 keys` 等调试信息，解析异常会在 UI 直接提示错误。

#### removeAfterFetch 处理
- 首次 `/jobs/result` 默认强制 `removeAfterFetch=false`，避免分页或二次渲染时被意外清空；若后端缺少专用清理接口，则保留缓存优先保证可用性。
- 同一 `jobId` 不会再二次调用带 `removeAfterFetch=true` 的 `/jobs/result` 自毁结果，后续分页/刷新直接复用首次返回的数据结构。

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
