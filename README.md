# SQL Notebook 用户操作指引

面向 Windows 11 风格的多标签 SQL 笔记本，基于 Java 22 + Swing + RSyntaxTextArea，支持 PostgreSQL / Hive 的编辑、执行与本地元数据缓存。

## 快速开始
1. 安装 JDK 22 与 Maven，并确保可以访问 Maven Central。
2. 运行 `mvn -DskipTests package` 生成可执行 fat JAR。
3. 使用 `java -jar target/sql_client-*-jar-with-dependencies.jar` 启动应用。

> 若容器/网络限制导致构建失败，可在联网环境下重新执行 Maven 下载依赖。

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
- 元数据字段列表：查询 SQL 已按 `ordinal_position` 排序，写入本地缓存的 `sort_no` 与该顺序一致，浏览器与联想均按此顺序读取。
- 查询结果展示：`SqlExecutionService` 优先读取响应中的 `columns`/`resultColumns` 数组作为列顺序；若缺失，则按后端返回的行对象顺序构建列清单，并用该顺序渲染 `QueryResultPanel`，避免 HashMap/JSONObject 打乱顺序。

### 配置与排错
- 相关配置集中在 `AsyncSqlConfig`，包含 BASE_URL、Token、AES Key/IV 等；证书忽略逻辑在 `TrustAllHttpClient`。
- 元数据或查询列顺序异常时，可开启操作日志观察“执行元数据 SQL”与任务完成日志，确认返回的列名顺序；必要时清空本地 SQLite（工具-重置元数据）后重试。

## 日志与元数据修复补充说明

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

### 结果集分页协议与配置
- 默认使用 `/api/jobs/result` 的分页参数：`page` 从 1 开始，`pageSize` 默认 2000，客户端会将配置值裁剪到 1~5000。
- 响应中的 `hasNext`、`truncated`、`note` 会在操作日志中输出，便于判断是否还有下一页、是否被截断以及后端裁剪提示。
- 普通 SQL 执行：仅拉取第 1 页并立即 `removeAfterFetch=true` 清理后端缓存，避免一次性加载超大结果；若需要保留分页能力，可在 `config.properties` 中设置 `allowPagingAfterFirstFetch=true`，但当前 UI 仍只展示首批数据。
- 元数据刷新：通过 `SqlExecutionService.executeSyncAllPages` 循环翻页直至 `hasNext=false`，并在最后一次请求后清理缓存，确保对象/字段抓取完整。
- 用户可在根目录或 classpath 的 `config.properties` 中编辑 `result.pageSize`，小于 1 时回退 2000，大于 5000 会在日志提示并裁剪到 5000。必要时可手动修改 `allowPagingAfterFirstFetch` 以便后端保留结果缓存供后续分页使用。
- 结果过期或 TTL 失效时会提示“结果已过期，请重新执行 SQL”，不会导致客户端崩溃；如需排查分页问题，可查看操作日志中的 page/pageSize/hasNext 信息。
