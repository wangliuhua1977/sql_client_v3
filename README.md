# SQL Notebook 用户操作指引

面向 Windows 11 风格的多标签 SQL 笔记本，基于 Java 22 + Swing + RSyntaxTextArea，支持 PostgreSQL / Hive 的编辑、执行与本地元数据缓存。目标数据库版本固定：PostgreSQL 12.7 on x86_64-pc-linux-gnu（gcc 4.8.5）。

## 快速开始
1. 安装 JDK 22 与 Maven，并确保可以访问 Maven Central。
2. 运行 `mvn -DskipTests package` 生成可执行 fat JAR。
3. 使用 `java -jar target/sql_client-*-jar-with-dependencies.jar` 启动应用。

> 若容器/网络限制导致构建失败，可在联网环境下重新执行 Maven 下载依赖。

### 构建命令（PowerShell）
- `mvn -q -DskipTests=false test`
- `mvn -q -DskipTests package`

## UI 设计与交互
- 主布局：左侧对象/笔记树（可折叠）、中间 SQL 编辑器、多标签或子窗体，底部为结果/消息分栏，整体由左右、上下双重 SplitPane 构成；右侧日志/历史/设置区默认隐藏，仅通过 View 菜单显式打开。
- 菜单结构：File（新建/导入/保存/关闭标签）、Edit（编辑器选项）、Run（执行/停止/标签切换）、View（区域显隐、布局重置、模式切换、焦点循环）、Tools（元数据/调试/备份/文本工具等低频入口）、Help（使用说明、快捷键）。
- 辅助面板默认隐藏：日志、历史、设置等低频区域初始不可见，仅在 View 菜单勾选后出现，状态可被 LayoutState 记住；对象浏览器保留在左侧，弹出式浏览器通过菜单或按钮打开。
- 快捷键表：

  | 快捷键 | 作用 |
  | --- | --- |
  | Ctrl+Enter | 执行当前 SQL/选中块 |
  | Ctrl+W | 关闭当前编辑标签或子窗体 |
  | Ctrl+Tab / Ctrl+Shift+Tab | 在标签/窗口间向前/向后切换 |
  | Ctrl+L | 聚焦编辑器输入区 |
  | Esc | 停止/取消正在执行的任务 |
  | F6 | 在对象树、编辑器、结果区之间循环焦点 |
  | Alt+W / Alt+Q | 打开 SQL 片段库 / 执行历史 |
  | Ctrl+, | 打开 SQL 编辑器选项 |

- 渲染器与 UIManager 收敛：全局字体统一为 13px，无衬线；JTree/JTable 行高约 22~24，选中背景为浅蓝；自定义树渲染器根据对象类型突出颜色与粗体，表格渲染器弱化网格、空值显示为空并强化选中态；UIManager 统一 SplitPane、ScrollPane 边框与 Tab 内边距，表头加粗、分隔线改为轻量 MatteBorder。

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
### 函数/存储过程源码编辑与调试运行
- 入口：在对象浏览器中右键 leshan schema 下的函数/存储过程节点，可选择“打开源码（只读）/编辑源码/运行/调试运行…”。同名重载会先列出 identity 参数供选择，内部以 OID 精确定位。
- 源码读取：通过异步 SQL `SELECT pg_get_functiondef(:oid) AS ddl;` 拉取完整 DDL 并在 SQL 编辑 Tab 打开，Tab 标题形如 `leshan.func(arg1 int)`，只读模式禁用发布按钮。
- 编辑/发布：在可编辑模式下直接修改 DDL，点击工具条上的“保存/发布”会原样执行 `CREATE OR REPLACE ...`（不包裹事务、不改写文本）。执行完成后刷新对象浏览器对应节点；失败时在结果面板展示错误并停止进度动画。
- 调试运行：右键选择“运行/调试运行…”弹出参数对话框，空值默认填 NULL，可直接输入 SQL 字面量。SQL 生成规则：
  - 存储过程：`CALL leshan.proc_name(arg1, arg2, ...);`
  - 函数：返回类型以 `SETOF ` 开头时生成 `SELECT * FROM leshan.func_name(...);`，否则生成 `SELECT leshan.func_name(...);`
  - 结果集与无结果集均复用现有结果渲染/提示链路。
- 错误定位：发布或运行失败时解析错误文本中的 `LINE n:` 或 `Position: n`，在编辑器内高亮对应行/字符偏移，解析不到则仅保留错误提示。
- 线程边界：源码拉取、发布与运行均复用既有异步 SQL 提交流程；网络/计算在后台线程完成，所有 Swing 组件更新（进度条、Tab 切换、光标定位、对话框弹出）均通过 `SwingUtilities.invokeLater` 回到 EDT。

## 临时笔记窗口（过程/函数查看）
- 触发场景：对象树中双击函数/存储过程节点、右键选择“打开源码（只读）/查看定义/Show DDL”等入口，或任何读取例程定义的动作，都会打开“临时查看：<schema.object()>”独立窗口。
- 默认行为：窗口内的编辑器允许查看与临时修改源码，但不自动保存、不写入笔记数据库、不记录最近笔记；关闭窗口直接丢弃更改，不弹保存确认。
- 保存为永久笔记：工具栏提供“保存为永久笔记…”按钮，点击后输入目标标题并确认，才会创建正式笔记并写入内容（可在主窗口继续编辑）。未点击前不会产生任何文件或数据库记录。

### 窗口与焦点
- 支持独立窗口模式与面板模式，子窗口可拖动、最大化/最小化或平铺。
- 程序启动会恢复上次的窗口集合，并自动把焦点放在最后一个打开的子窗口。
- 工具栏的“执行/停止”按钮随当前焦点窗口的执行状态切换；切换焦点即可控制对应窗口的 SQL 请求启动或中止。

### 智能联想模式
- Alt+E 全局切换联想开关，状态栏显示“联想: 开启/关闭”，无需在光标附近弹提示气泡。
- 激活时，在输入表/视图、字段、函数或存储过程位置自动弹出联想列表，连续输入会保持弹窗开启；敲回车选择或在语句末尾输入分号会收起弹窗。

### SQL 执行
- Ctrl+Enter 会执行选中内容或光标所在的整个 SQL 代码块（空行分块），块内语句按顶层分号拆分后串行提交，结果出现在当前窗口或底部共享结果区。
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

## 结果集表格复制（Swing JTable）
- 触发入口：在结果集 JTable 中按 `Ctrl+C`，或使用右键菜单的“复制选中单元格/复制选中列内容/复制选中列名（逗号分隔）”。
- 复制格式：单元格与多单元格复制使用 `\t` 分隔同一行的列，`\n` 分隔不同行，`null` 视为 **空字符串**。列名复制使用英文逗号+空格（如 `colA, colB`），列值复制遵循当前表格已加载的数据，不触发额外网络请求。
- 选区规则：支持不连续多选（行/列/单元格皆可），右键点击未选中的单元格会先将焦点移动到该单元格，列复制会优先使用选中的列；若未显式选列，则使用当前焦点列。
- 边界行为：表格无数据或无有效选区时菜单项自动禁用；复制动作写入系统剪贴板（AWT Clipboard）。所有 UI 更新与剪贴板操作均在 EDT 触发或通过 `SwingUtilities.invokeLater` 派发，避免线程安全问题。

## 执行进度条/Loading 状态收敛（Swing）
- 统一入口：`QueryResultPanel#setBusy(boolean)` 统一控制执行态，`busy=true` 时进度条可见且处于动画/不定模式，`busy=false` 时停止动画并隐藏。
- 收敛时机：
  - 提交/队列或轮询更新：收到 QUEUED/RUNNING 等状态会自动置为 `busy=true`。
  - 终态（SUCCEEDED/FAILED/CANCELLED 等）或渲染完成（包含结果集、非查询信息、错误提示）后调用 `setBusy(false)`，确保无漏关。
  - 异常/失败：展示错误提示后同样关闭动画，不会因异常提前返回而悬停。
- 线程规则：所有 `setBusy` 和状态标签更新均通过 EDT 执行，避免跨线程修改 Swing 组件。

## SQL 编辑器滚动条滚轮支持（子窗体/面板模式）
- 适用范围：`EditorTabPanel` 作为独立子窗体（JInternalFrame/JDialog/JFrame）或主界面内嵌面板（tabbed 面板模式）时，SQL 编辑区无论鼠标停在正文编辑区域还是右侧竖向滚动条上，滚轮都能驱动同一垂直滚动条滚动。
- 实现方式：封装工具类 `tools.sqlclient.ui.swing.ScrollBarWheelSupport#enableWheelOnVerticalScrollBar(JScrollPane)`，在 SQL 编辑器的 `RTextScrollPane` 上安装 `MouseWheelListener`，仅监听垂直 `JScrollBar`，不改动编辑器本体的原生滚轮行为。
- 方向与步进：使用 `MouseWheelEvent#getUnitsToScroll()` 与滚动条 `getUnitIncrement`（回退 `getBlockIncrement`）计算步进，向下滚动（值大于 0）使滚动条 value 增大、内容下移，向上滚动相反；结果值在 `minimum` 与 `maximum - visibleAmount` 之间夹紧。
- 绑定与线程：通过滚动条 `clientProperty` 标记避免重复绑定，初始化时若不在 EDT 会使用 `SwingUtilities.invokeLater` 派发安装，确保监听注册与 UI 更新在事件派发线程完成。

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

## 前端开发技术文档：结果集列对齐与渲染
### ResultSet columns/rows 协议（/jobs/result）
- **columns 必须来自 JDBC ResultSetMetaData**，禁止在服务端解析 SQL 文本自行推断 `*`/`alias.*` 展开顺序。
- columns 结构要求（字段可选但必须稳定）：`[{index(从1开始), name(原始列label), displayName(用于 UI 去重), dbType, tableName?, schema?, nullable?}]`
- rows 必须是 **数组形式**：`[[v1, v2, ...], ...]`，并与 columns 顺序严格一致，支持重复列名。
- 兼容旧字段：若存在 `rowsObject/resultRows`（Map/对象形式），客户端仅作为兜底读取；新前端始终优先使用 `rows` 数组。
- archived=true 场景必须同样返回 columns + rows，保证分页重入可用。

### displayLabel 与 dataKey 的分工
- `displayLabel`：用于 JTable 表头展示（来自 SQL 投影/别名/服务端 label），不参与取值对齐。
- `dataKey`：用于从结果行中取值的真实 key（来自服务端 columns/columnMetas 或 raw rows 的实际 key）。
- `ColumnDef` 内部使用 `displayName` 作为显示名，`sourceKey` 作为 dataKey；两者可不同以避免无别名表达式错位。

### 行值对齐策略（数组/对象 + fallback）
- **行是数组/列表**：按列索引直接取 `row[i]`（唯一可信路径，支持重复列名）。
- **行是对象/Map**：优先用 `dataKey` 取值；取不到时再尝试 `displayLabel`（大小写不敏感）。
- **fallback 顺序**：
  1. `dataKey` 精确匹配；
  2. `dataKey` 大小写不敏感匹配（记录 debug）；
  3. `displayLabel` 精确/大小写不敏感匹配（记录 debug）；
  4. 若 Map 为有序且 `row.size == columnCount`，按插入顺序取第 i 个值（记录 debug）；
  5. 若只有 1 列且 Map 只有 1 个值，取唯一值（记录 debug）。
- 所有 fallback 仅在无法命中 key 时触发，并且 debug 日志为一次性输出，避免刷屏。
- **重复列名处理**：当 columns 中存在重复列名且 rows 仍为 Map 时，客户端会记录提示并避免错误重复取值（重复列将保持空值），提示后端必须改为 rows 数组。

### 手工回归脚本（JOIN 重复列名）
请在同一库中准备以下临时表并执行 SQL，验证列顺序与 count_* 列必须出现，且重复列按列序号对齐：
1. `select * from tmp_wlh_jk2;`
2. `select * from tmp_wlh_jk3_2;`
3. `select * from tmp_wlh_jk2 a left join tmp_wlh_jk3_2 b on a.prod_inst_id=b.prod_inst_id;`
4. `select a.*, b.* from tmp_wlh_jk2 a left join tmp_wlh_jk3_2 b on a.prod_inst_id=b.prod_inst_id;`
期望行为：`a.*` 列在前、`b.*` 列在后；右表 `count_all/count_zj/count_bj` 必须出现；若列名重复，表头自动去重显示（如 `acc_nbr`、`acc_nbr#2` 或 `a.acc_nbr`、`b.acc_nbr`），但值仍按列序号严格对齐。

### SQL 示例（无别名表达式也可稳定显示）
- SQL1（无别名）：`select sum(amount)/100 from tmp_wlh_mbhs1`
  - 显示列头为 `sum(amount)/100`，单元格正常展示数值（例如 `272681.8`）。
- SQL2（显式别名）：`select sum(amount)/100 as amount from tmp_wlh_mbhs1`
  - 显示列头为 `amount`，单元格展示与 SQL1 一致。

### 测试用例与运行方式
- 新增单测：`src/test/java/tools/sqlclient/exec/RowValueResolverTest.java`
- 运行命令：`mvn -q -DskipTests=false test`

## 前端开发技术文档：元数据获取与列顺序

### 可空数值与 OptionalInt 转换规范
- OptionalInt 转 Integer 时使用显式 `isPresent()` + `getAsInt()` 判断，例如：`OptionalInt parsed = ...; if (parsed.isPresent()) { return parsed.getAsInt(); }`，否则返回 null；禁止使用 `orElse(null)` 等向 primitive 传入 null 的写法。
- 禁止将 null 直接传给 primitive 参数或依赖自动拆箱；所有可能为空的数值一律使用包装类型或显式默认值（如三元表达式或 `Objects.requireNonNullElse`）处理。

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
- 函数：`SELECT n.nspname AS schema_name, p.proname AS object_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='leshan' AND p.prokind IN ('f','w') ORDER BY p.proname OFFSET :offset LIMIT 1000`。
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
- 失败信息透传：无论 SELECT 还是非 SELECT，只要 `success=false`，客户端都会使用服务端返回的 `errorMessage/message/note` 作为结果面板与 OperationLog 的文案，避免“失败但无详情”的情况。非查询失败同样会展示完整错误原因。

## 错误展示优先级与表格展示规则
- 判断 SQL 执行报错：响应状态为 `FAILED`，或响应中存在 `errorMessage`/`position`/`Position` 字段时，结果表格会切换到错误行展示模式。
- 错误正文优先级：
  1. 第一行展示 `errorMessage`（非空时）。
  2. 若存在位置偏移（`position` 或 `Position` 大于 0），第二行追加 `Position: <数字>`。
  3. 若 `errorMessage` 为空，会回退使用 `error.raw`/`error.message` 或响应体中的 `message`。
  4. 所有字段均缺失时，使用固定文案“数据库未返回错误信息”，不再将 `jobId/status` 填入结果表格正文。
- 表格渲染器支持多行换行，错误正文会直接显示在结果表格的错误列/行中，无需额外弹窗。

## 失败结果表格化展示
- 失败状态与成功路径一致，会在结果集区域生成“结果1/结果N”标签页展示错误详情，而非仅渲染单行状态文本。
- 失败结果的表格列固定为：`ERROR_MESSAGE`、`POSITION`，至少包含一行数据；`ERROR_MESSAGE` 保留后端原始换行。
- `POSITION` 取值优先级：1）`position` 字段；2）`Position` 字段；3）正则解析 `errorMessage` 中的 `Position: <number>`（不区分大小写）。解析不到则留空。
- 严禁在表格正文填入“任务<jobId> 状态 FAILED”或“SQL 执行失败: <sql> | 任务...FAILED”等拼接文本，这类文案仅用于日志或状态栏。
- 样例：后端返回
  ```json
  {
    "status": "FAILED",
    "sqlSummary": "select * from t1",
    "hasResultSet": false,
    "errorMessage": "ERROR: relation \"t1\" does not exist\n  Position: 30",
    "jobId": "44908dbf-397d-407c-b425-9103dd9a4ff3"
  }
  ```
  前端结果表格渲染：
  | ERROR_MESSAGE | POSITION |
  | --- | --- |
  | ERROR: relation "t1" does not exist (换行保留) | 30 |

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

### 前端开发技术文档（元数据模块）
- **启动链路**：先读取 `metadata_snapshot` 做对象级签名比对；签名一致则跳过远端拉取，签名不一致时按 900 行分页拉取对象清单、差分写入本地 SQLite，并回写新的 snapshot。
- **手动刷新**：菜单触发的刷新与启动链路一致，仅同步对象列表（table/view/function/procedure），不会拉取字段、也不会调用 `ensureColumnsLoaded`。
- **字段懒加载**：只有在联想/列顺序/对象浏览器需要某表字段且本地 columns 缓存为空时，才调用 `ensureColumnsLoaded(schema, table)` 向远端查询。若缓存命中直接返回，缓存缺失会创建/复用以 `schema.table` 为键的 in-flight Future，成功写入后刷新 UI。
- **去重策略**：对象级跳过依赖 `metadata_snapshot`，列级避免重复依赖 columns 表缓存命中；同一表的并发请求会被 in-flight map 复用，失败后自动清理以便下次重试。
- **字段 SQL（PG12.7 兼容）**：`SELECT ... FROM pg_attribute a JOIN pg_class c ... WHERE n.nspname=:schema AND c.relname=:table ORDER BY a.attnum;` 单表查询无需分页且按 `attnum` 稳定排序，参数在拼接前会做标识符校验/转义。
- **SQLite 结构**：objects(schema_name, object_name, object_type, use_count, last_used_at, PK(schema_name, object_name))；columns(schema_name, object_name, column_name, sort_no, use_count, last_used_at, PK 同步，索引 `idx_columns_schema_object` 支持 `(schema,table)` 快速命中)；columns_snapshot(object_name, cols_hash, updated_at)；metadata_snapshot(object_type, remote_count, remote_signature, checked_at)；meta_snapshot 仅存储补充信息哈希。
- **清理与刷新**：差分同步或 DDL 删除对象时会同步删除 columns/columns_snapshot 记录；手动“重置元数据”仅清空本地缓存，后续依然按对象差分 + 懒加载列重新写库。
- **关键日志**：OperationLog 会输出 `[meta] columns cache hit/miss`、`inflight reused`、`fetched count` 或失败原因，元数据刷新进度日志包含批次、对象/列计数与耗时，便于排障。

### 启动全量同步
- 主窗口完成初始化后即触发异步元数据刷新，`meta_snapshot` 为空时执行全量同步；后续刷新仅更新变动的类别/对象。
- 清空本地缓存（菜单“工具-重置元数据”）会删除 `objects/columns/meta_snapshot`，强制下一次重新全量拉取。

### 操作日志降噪
- 笔记链接同步的成功日志已降级为 DEBUG，不再通过右侧“操作日志”面板输出“更新链接/Update link”等文案；如需排查可改用文件日志（`NoteRepository`）。
- 仍保留解析/保存失败的提示，且改为“保存笔记关联失败”以便快速定位异常。

## 错误模型与展示规则

异步接口返回的错误统一落在 `error` 对象中，客户端不会再将“任务状态 FAILED”当作正文。

- **字段定义**
  - `message`: 数据库原始错误信息（PG/JDBC message）
  - `sqlState`: `SQLException.getSQLState()`
  - `vendorCode`: `SQLException.getErrorCode()`
  - `detail/hint/position/where/schema/table/column/datatype/constraint/file/line/routine`: `PSQLException.getServerErrorMessage()` 中的对应字段，缺失则为 `null`
  - `raw`: 直接可展示的多行文本，拼接规则如下
- **raw 拼接规则**
  1. 第一行：`message`
  2. 若存在 `sqlState`：追加一行 `SQLSTATE: <sqlState>`
  3. 若存在 `position`：追加一行 `Position: <position>`
  4. 依次追加 `Detail:`、`Hint:`、`Where:` 行（有值才追加）
- **前端展示优先级**
  1. `error.raw`
  2. 旧字段（如顶层 `errorMessage`/`message`）
  3. 兜底状态文本（例如 `FAILED`），不再作为正文
- **示例**
  - 语法错误：
    - `message`: `syntax error at or near "asselect"`
    - `sqlState`: `42601`
    - `position`: `123`
    - `raw` 展示：
      ```
      syntax error at or near "asselect"
      SQLSTATE: 42601
      Position: 123
      ```
  - 约束错误：
    - `message`: `duplicate key value violates unique constraint "users_pkey"`
    - `sqlState`: `23505`
    - `detail`: `Key (id)=(1) already exists.`
    - `constraint`: `users_pkey`
    - `raw` 展示：
      ```
      duplicate key value violates unique constraint "users_pkey"
      SQLSTATE: 23505
      Detail: Key (id)=(1) already exists.
      ```
  - 约定：错误正文始终使用数据库返回的原始文本，不再显示“任务状态 FAILED”或其他抽象描述。

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
- **removeAfterFetch 语义**：`removeAfterFetch=true` 时，后端在成功返回后会从内存缓存移除该 job，再次请求将返回 404。客户端仅在“确定不再翻页/重试”时才发起一次最终带 `removeAfterFetch=true` 的 `/jobs/result`，轮询阶段统一使用 `/jobs/status`，避免“先返回 200 再被 404 覆盖”的双请求。元数据刷新与普通查询都遵循该规则。

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
- 元数据/短结果优先保证可重试性：轮询阶段仅用 `/jobs/status`，最终确认无需再翻页时才发送一次 `removeAfterFetch=true` 的 `/jobs/result`，避免“先拉取成功又被 404 覆盖”。默认 `AUTO` 不会额外追加清理请求。
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
- 轮询与重试：`waitUntilReady` 统一使用 `/jobs/status` 轮询，终态后只发送一次 `/jobs/result`（带 `removeAfterFetch=true`）获取最终数据或错误详情，避免“已移除再 404”的二次拉取。单批超时 60s；overload 采用 500ms 起步指数退避。
- 取消：`MetadataService.cancelRefresh` 设置取消标记并调用 `/jobs/cancel`（reason=`Cancelled by UI`），取消后 staging 不会覆盖正式表。
- 本地安全：结果先写入 `objects_staging/columns_staging`，成功后一次性 `swapFromStaging`；失败/取消保留旧缓存。
- PG12 系统表兼容：针对 `pg_proc` 采用 `prokind IN ('f','w')` 过滤函数，聚合使用 `prokind='a'`，不再引用已废弃的 `proisagg` 字段，确保 SQL 可在 PostgreSQL 12.7 正常执行。

### 常见错误与排障
- 401：Token 错误或缺失，`RemoteSqlClient` 会抛出 “HTTP 401 未授权” 并在 OperationLog 打印请求/响应；请确认 `X-Request-Token`。
- 410/`RESULT_EXPIRED`：结果已过期，UI 提示“结果已过期”，元数据刷新会跳过覆盖，可重新执行提交。
- `RESULT_NOT_READY`：表示结果仍在落盘，客户端会继续轮询；日志会显示 attempt/page 信息。
- AES 解密失败：检查 Key/IV 与 UTF-8，参考 `AesCbcTest` 或下方 PowerShell 脚本生成密文。
- pageSize/limit >1000：客户端自动裁剪并在 `ResultResponse.note` 与日志中提示裁剪值。

### 前端开发技术文档（编辑器交互与临时笔记生命周期）
#### 联想窗口关闭策略
- 统一入口：`SuggestionEngine#hidePopup(reason)` 是唯一关闭方法，保证幂等；所有事件统一走该入口。
- 触发条件：
  - 编辑器失焦（`RSyntaxTextArea` 的 `focusLost`）。
  - 光标位置改变且不是联想插入动作（`CaretListener` + `suppressCaretDismiss` 时间窗）。
  - 弹窗显示时的全局鼠标点击（`AWTEventListener`，排除弹窗自身与编辑器组件）。
  - Tab/窗口切换导致 active editor 变化或窗口失活（`MainFrame#syncActivePanelWithSelection` 与 `windowDeactivated`）。
- 稳定性要点：联想插入前设置 `suppressCaretDismiss`，插入后 `invokeLater` 复位，避免“提交候选立刻关弹窗”抖动。

#### 临时笔记默认命名规则与实现位置
- 新建临时笔记不弹出命名对话框，标题由 `UntitledNoteNamer` 统一生成（`tools.sqlclient.util.UntitledNoteNamer`）。
- 默认格式：`未命名 1/2/3...`（AtomicInteger 递增，进程内不回收）。
- 命名生成入口：`MainFrame#createNote` → `UntitledNoteNamer.nextTitle(...)` → `createTemporaryNote`。

### 前端开发技术文档：临时笔记 / 正式笔记与滚轮滚动
#### 状态字段与生命周期
- 状态字段：`Note.temporary` 标记临时笔记；`EditorTabPanel` 维护 `dirty` 与 `initialContent`，用于判断是否需要关闭提示。
- 临时笔记：
  - 新建笔记与存储过程/函数查看均创建 `Note.temporary=true` 的临时笔记（`MainFrame#createTemporaryNote`、`TemporaryNoteWindow`）。
  - 持久化策略切换为 `TemporaryNotePersistenceStrategy`，自动保存与链接持久化关闭，内容只保留在内存中。
  - 标题显示追加 “(临时)” 标记（`MainFrame#getPanelDisplayTitle`）。
- 正式笔记：
  - 通过 `NoteRepository.create` 写入 SQLite，并启用 `PersistentNotePersistenceStrategy` 自动保存与链接持久化。
  - 保存后 `Note.temporary=false`，后续保存/关闭与正式笔记一致。

#### 关键流程（文字流程图）
1) **新建笔记**
   - 用户触发“新建笔记” → `MainFrame#createNote` → `createTemporaryNote` 创建临时 Note → `EditorTabPanel` 使用临时持久化策略 → 打开标签/窗口。
2) **读取存储过程/函数**
   - `MainFrame#showRoutineDdlAndOpen` 获取 DDL → `TemporaryNoteWindow` 展示临时笔记 → 需要另存为正式笔记才会写入仓库。
3) **保存为正式笔记**
   - 用户点击“保存当前”或关闭临时标签 → `MainFrame#saveTemporaryPanel` 弹出命名对话框 → `NoteRepository.create` + `updateContent` → `EditorTabPanel#replaceNote` 切换到正式持久化策略。

#### 关闭弹窗规则
- 临时笔记关闭：
  - `dirty=false`（内容未改动或与初始一致）→ 直接关闭，无弹窗。
  - `dirty=true` → 弹出三按钮对话框（保存为正式笔记 / 不保存 / 取消）。
- 保存失败：弹窗提示失败原因，临时状态与内容保持不变。
- 关闭取消：选择“取消”则保持窗口/标签打开，不触发任何保存或清理。
- 临时缓存清理：当前实现不写入磁盘临时缓存，关闭时无额外清理动作。
- 选择“不保存”时：临时笔记不写入正式索引，直接释放内存状态。

#### 滚轮事件路由（SQL 编辑区）
- 监听点：`EditorWheelScrollSupport.install` 在编辑器组件上挂载 `MouseWheelListener`。
- 路由策略：事件触发时用 `SwingUtilities.getAncestorOfClass(JScrollPane.class, source)` 查找最近滚动容器，优先驱动垂直滚动条并 `consume()`，确保“鼠标悬停即可滚动”。
- 不使用全局监听：避免误抢其他区域（结果表格、树等）的滚轮滚动事件。

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

## 前端执行链路与规则补充

### 前端开发者详细技术文档 - /jobs/result 取结果与分页
- **请求体（POST /jobs/result）**：统一使用 `jobId + offset + limit (+ removeAfterFetch?)`，其中 limit 默认 200，前端需将 limit/pageSize 限制在 1~1000，offset 最小 0。未显式提供 offset/limit 时，可按 `offset=(page-1)*pageSize`、`limit=pageSize` 的换算规则生成；`removeAfterFetch` 默认 false，仅移除内存缓存，不影响归档。
- **持久化优先/可重入分页**：归档命中时 status=SUCCEEDED 且 `resultAvailable=true`，同一 jobId 可重复传入不同 offset/limit 翻页；超过 1000 的窗口自动被后端截断，前端应在 hasNext=false 或 offset+limit>maxVisibleRows 时禁用继续翻页。
- **响应字段映射**：`success/status/code/message/errorMessage` 负责提示；`hasResultSet/resultAvailable` 决定是否渲染网格；`columns`（数组，包含 name/type/position/jdbcType/precision/scale/nullable）仅出现 1 次，必须按顺序生成列头，即便 `returnedRowCount=0` 或 `resultRows=[]` 也要显示列名；`resultRows` 为对象数组，key 与 columns.name 对齐；`returnedRowCount/maxVisibleRows/maxTotalRows/hasNext/truncated/page/pageSize/offset/limit` 用于记录数与分页/截断提示；`rowsAffected/updateCount/commandTag` 用于非 SELECT 的影响行数；`submittedAt/finishedAt/durationMillis/archivedAt/expiresAt/lastAccessAt/sqlSummary/dbUser/label/archived/archiveStatus/archiveError` 直接透传到 UI 状态栏/日志。
- **错误码与 HTTP 状态**：HTTP 410 或 `code=RESULT_EXPIRED` 需提示“结果已过期”（可携带 expiresAt）；HTTP 404 显示 jobId 不存在；HTTP 401 提示 Token 失败；HTTP 400 直接显示 errorMessage/message/code；`code=RESULT_NOT_READY` 视为未完成继续轮询；FAILED/CANCELLED 使用 `errorMessage > message > code > Unknown error` 的优先级。
- **示例（含空结果集也包含 columns）**：
```json
{
  "success": true,
  "jobId": "uuid-1",
  "status": "SUCCEEDED",
  "hasResultSet": true,
  "archived": true,
  "archiveStatus": "ARCHIVED",
  "resultAvailable": true,
  "returnedRowCount": 2,
  "truncated": false,
  "hasNext": false,
  "page": 1,
  "pageSize": 200,
  "offset": 0,
  "limit": 200,
  "maxVisibleRows": 1000,
  "columns": [
    { "name": "col", "type": "varchar", "position": 1, "jdbcType": 12, "precision": 255, "scale": 0, "nullable": 1 },
    { "name": "amount", "type": "numeric", "position": 2, "jdbcType": 2, "precision": 18, "scale": 2, "nullable": 0 }
  ],
  "resultRows": [
    { "col": "v1", "amount": 10.50 },
    { "col": "v2", "amount": 11.30 }
  ],
  "submittedAt": 1717142400000,
  "finishedAt": 1717142410000,
  "durationMillis": 10000,
  "archivedAt": 1717142410500,
  "expiresAt": 1717149010500,
  "lastAccessAt": 1717142410500,
  "sqlSummary": "select ...",
  "dbUser": "leshan"
}
```

### 结果集字段兼容表
- `/jobs/result` 响应的列名接受 `columns/resultColumns/columnNames/columnDefs`，行数据接受 `resultRows/rows/data/records`。
- 计数与分页字段兼容 `totalRows/total/totalCount/returnedRowCount/actualRowCount`，`hasResultSet` 缺失时复用 `resultAvailable`。
- `offset/limit/page/pageSize` 均会被日志输出，首批强制 `offset=0`。

### 列名自动修复规则
- 简单聚合：`sum(col)`→`sum_col`，`avg(col)`→`avg_col`，`min/max(col)` 同理；`count(*)`→`count_all`，`count(1)`→`count_1`，`count(col)`→`count_col`。
- 复杂 `sum(...)`（含空格/CASE/DISTINCT/运算符/嵌套等）统一归一为 `sum_`、`sum_2`、`sum_3...`，优先尊重显式别名。
- 其他表达式按“全小写、非字母数字转 `_`、压缩连续 `_`、首字符非字母前置 `expr_`、全局去重 _2/_3”生成；完全无信息时兜底 `col_1/col_2...`。

### SQL 代码块定义（Ctrl+Enter）
- 代码块边界：文件首尾与空行（`trim()` 为空的行）都是硬边界，空行不属于任何代码块。
- 块内语句边界：仅顶层分号 `;` 拆分语句，忽略字符串、`--` 行注释、`/* */` 块注释以及 `$$...$$`/`$tag$...$tag$` 内的分号；块内最后一条语句可不以分号结束。
- Ctrl+Enter 行为：
  - 有选区：选区视为虚拟代码块，按从上到下顺序逐条执行其中的语句。
  - 无选区：定位光标所在代码块并执行整块；若光标位于空行，优先选择下方最近的块，没有则选择上方最近的块；两侧都没有则不执行。
  - 执行顺序：单个代码块内的语句会依次提交（statement[0]→statement[1]→...），某条失败立即中止并展示数据库原始错误信息。
- 示例：
  - 空行隔离：
    ```
    create table tmp_wlh_vvvv1 as
    select * from mt_icb_offer_inst_attr

    select * from tmp_wlh_jk3;
    ```
    光标落在第二段 `select` 内时仅执行第二个代码块；落在 `create table` 段时仅执行第一个代码块。
  - 同一块多语句：
    ```
    select 1; select 2;
    ```
    光标在 `select 2` 任意位置触发 Ctrl+Enter，会按顺序执行 `select 1` 与 `select 2`。

### 日志定位方法
- 结果集请求会输出 `JOB_RESULT_FETCH: jobId=... offset=... limit=...`，便于排查分页是否正确。
- 结果解析会记录列数/行数/totalRows 统计、是否截断、线程池快照等。

## 前端开发技术文档：粘贴导入 PG

### 功能入口与用户步骤
- 工具菜单新增“粘贴导入”（Ctrl+Shift+V），打开 `ExcelPasteImportDialog`。默认单页：左侧粘贴区（Ctrl+V 捕获剪贴板）、右侧预览区，底部字段设置默认展开可折叠。
- 推荐流程：粘贴 Excel 选区（含表头）→ 等待自动采样/预览 → 点击“导入”。如需调整列名/类型/勾选，可直接在底部“字段设置”表格修改或使用“重置推断”。

### 表名规则与避让
- 默认表名输入框初始 `tmp_wlh_import_1`；导入时按用户输入为基名，在当前 Schema（下拉可编辑，默认 leshan/public）下检测是否已存在。
- 若存在同名表，自动附加 `_1/_2...` 递增直到不存在，并在状态栏提示“最终表名”。建表/插入均使用该最终名称。

### 大批量处理策略
- 剪贴板内容上限 20MB，HTML 通道单独限制 5MB；超限直接报错拒绝，避免一次性持有超大字符串导致 OOM。
- 粘贴后立即将内容落盘为 UTF-8 TSV 临时文件（`java.io.tmpdir`，导入完成后删除），不保留全量二维列表；仅预览前 20 行在内存中。
- 类型推断仅对前 5000 行做快速采样以渲染 UI，随后后台以 BufferedReader 流式全量扫描临时文件淘汰候选类型，仅保存计数/最大长度，不缓存原值。
- 导入流式读取临时文件，跳过表头；批量 INSERT 默认 200 行，并基于 SQL 长度上限 512KB 自动提前 flush，避免单条语句过大。
- 支持取消：对话框“取消”会中断后台 SwingWorker，终止后回滚事务并清理临时文件。

### 解析与回退策略
- 优先尝试剪贴板 `text/html` 表格，通过 `ParserDelegator` 流式解析 `<tr>/<td>` 转为 TSV；若 HTML 超限或无表格则回退 `text/plain`（Tab 分隔）。
- 行列对齐：以首行表头为起点，后续行若列数增加会自动补充 `col_n` 列名，缺失列补空字符串。
- 表头规范化：去空白、转小写、非 `[a-z0-9_]` 替换为 `_`，数字开头前缀 `c_`，常见保留字前缀 `c_`，重名追加 `_2/_3...`。

### 类型推断规则
- 采样阶段与全量阶段均使用候选淘汰法：每列初始候选 boolean/int/bigint/numeric/date/timestamp/uuid/jsonb/varchar/text，遇到不匹配的值立即移除候选，记录非空计数与最大长度。JSON 仅在值以`{`或`[`开头且解析成功时才计入，否则会立刻淘汰 jsonb 候选，避免误判普通文本/数字列；且至少需要 3 个非空值通过校验才会选择 jsonb。
- 全量扫描完成后按优先级选择最终类型：boolean > integer > bigint > numeric > date > timestamp > uuid > jsonb（需 ≥95% 非空值可解析 JSON 且满足最少样本）> varchar(<=255) > text。JSON 校验使用 Gson 解析，失败即淘汰。
- 字段表新增来源列：auto(采样)/auto(全量)/user。用户改动目标字段名/类型即锁定（user），后续全量推断不会覆盖。目标名为空/非法（非 `[a-zA-Z_][a-zA-Z0-9_]*`）或重复时行内标红且禁用“导入”。
- 其余使用 `varchar` 或 `text` 兜底；字段表仅允许选择上述无精度类型。若用户输入了 `numeric(10,2)` / `varchar(255)` 等，会自动规范化为无精度版本并在状态栏提示一次。

### 导入策略与执行
- 导入策略下拉：自动适配（默认）、直接追加、清空插入、删除后重建。
  - 自动适配：存在表时先拉取 information_schema.columns 判断列名与类型可否兼容（numeric/int4/int8 一组、varchar/text 一组、date/timestamp 允许互转），兼容则 TRUNCATE 再插入，否则 DROP+CREATE。
  - 直接追加：仅 INSERT；若目标表不存在会降级为删除后重建并提示。
  - 清空插入：存在表则 TRUNCATE+INSERT，不存在则 CREATE+INSERT。
  - 删除后重建：无论是否存在均 DROP IF EXISTS 后 CREATE+INSERT。
- 顺序：BEGIN → 策略处理（可 DROP/TRUNCATE/CREATE）→ 批量 INSERT → COMMIT。失败时 ROLLBACK，若本次新建则回收临时表；错误消息透传 PG 返回的 raw/Position/SQLSTATE，并标注失败批次行号范围。
- 值写入：按列勾选与目标类型生成 INSERT，空值写 NULL；数值/时间/布尔/UUID/JSONB 均使用 `CAST('value' AS type)` 明确报错位置，始终显式列名避免列顺序差异。
- 进度：进度条基于采样行数，批次插入时实时更新“已处理 N 行”；取消或异常均提示状态并停止后续批次，可直接修改策略/表名/映射后再次点击导入，无需重新粘贴。

### 重试与重置
- 导入失败后会保留临时文件、采样结果与字段映射，允许直接再次点击“导入”或调整策略/表名后重试。
- 底部新增“重置”按钮：清空粘贴区与预览、删除临时文件、恢复默认表名与字段推断。仅在主动重置或关闭窗口时才清理临时文件。

### 粘贴区预览
- 粘贴区上方新增“粘贴内容预览（前20行）”只读区域，展示标准化对齐后的表头+前 19 行（已按 TSV 解析），与右侧 JTable 预览一致，避免 HTML/TSV 差异。

### 关键类与职责
- `tools.sqlclient.ui.ExcelPasteImportDialog`：主对话框，负责剪贴板落盘、采样推断、预览渲染、字段设置、建表/插入及进度与取消处理。
- 复用 `SqlExecutionService` 提交 DDL/DML；`OperationLog` 记录解析通道、最终表名、建表 SQL、批次执行/失败原因。
- 临时文件管理与大对象引用在导入完成/取消后清理，防止 SwingWorker 持有全量数据。
