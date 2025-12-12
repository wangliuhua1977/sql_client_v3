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
