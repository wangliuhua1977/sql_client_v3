package tools.sqlclient.exec;

import com.google.gson.JsonArray;
import tools.sqlclient.remote.RemoteSqlClient;
import tools.sqlclient.remote.RemoteSqlConfig;
import tools.sqlclient.util.Config;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.ThreadPools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 基于异步任务接口的 SQL 执行服务：提交任务、轮询状态、获取结果。
 */
public class SqlExecutionService {
    private static final int DEFAULT_MAX_RESULT_ROWS = 0; // 分页模式下不再按总行数截断
    private static final int DEFAULT_PAGE_SIZE = 200;
    private static final int MAX_PAGE_SIZE = Math.min(RemoteSqlConfig.SERVER_MAX_PAGE_SIZE, Math.max(1, Config.getMaxPageSize()));
    private static final int MAX_ACCUMULATED_PAGES = 20;
    private static final int MAX_ACCUMULATED_ROWS = 100_000;
    private final RemoteSqlClient remoteClient = new RemoteSqlClient();

    /**
     * 同步执行一条 SQL：提交后台任务，轮询至结束并返回结果集。
     * 仍然复用异步接口的加密与 Header 逻辑。
     */
    public SqlExecResult executeSync(String sql) {
        return executeSync(sql, DEFAULT_MAX_RESULT_ROWS);
    }

    public SqlExecResult executeSyncWithDbUser(String sql, String dbUser) {
        return executeSync(sql, DEFAULT_MAX_RESULT_ROWS, false, dbUser);
    }

    public SqlExecResult executeSyncWithoutLimit(String sql) {
        return executeSync(sql, null);
    }

    public SqlExecResult executeSyncAllPages(String sql) {
        return executeSync(sql, null, true);
    }

    public SqlExecResult executeSyncAllPagesWithDbUser(String sql, String dbUser) {
        return executeSync(sql, null, true, dbUser);
    }

    public SqlExecResult executeSyncAllPagesWithDbUser(String sql, String dbUser, Integer pageSizeOverride) {
        return executeSync(sql, null, true, dbUser, pageSizeOverride);
    }

    public SqlExecResult executeSync(String sql, Integer maxResultRows) {
        return executeSync(sql, maxResultRows, false);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages) {
        return executeSync(sql, maxResultRows, fetchAllPages, null);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages, String dbUser) {
        return executeSync(sql, maxResultRows, fetchAllPages, dbUser, null);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages, String dbUser, Integer pageSizeOverride) {
        int resolvedMaxRows = determineMaxRows(maxResultRows, pageSizeOverride);
        AsyncJobStatus submitted = submitJob(sql, resolvedMaxRows, dbUser, null);
        try {
            AsyncJobStatus finalStatus = pollJobUntilDone(submitted.getJobId(), null).join();
            if (!"SUCCEEDED".equalsIgnoreCase(finalStatus.getStatus())) {
                throw new RuntimeException(finalStatus.getMessage() != null
                        ? finalStatus.getMessage()
                        : ("任务" + finalStatus.getJobId() + " 状态 " + finalStatus.getStatus()));
            }
            return requestResultPaged(submitted.getJobId(), sql, fetchAllPages, pageSizeOverride, finalStatus);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("执行 SQL 失败: " + cause.getMessage(), cause);
        }
    }

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError) {
        return execute(sql, null, null, onSuccess, onError, null);
    }

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError,
                                           Consumer<AsyncJobStatus> onStatus) {
        return execute(sql, null, null, onSuccess, onError, onStatus);
    }

    public CompletableFuture<Void> execute(String sql,
                                           String dbUser,
                                           Integer pageSize,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError,
                                           Consumer<AsyncJobStatus> onStatus) {
        String trimmed = sql == null ? "" : sql.trim();
        if (trimmed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        OperationLog.log("即将提交异步 SQL:\n" + abbreviate(trimmed));

        return CompletableFuture.supplyAsync(() -> submitJob(trimmed, determineMaxRows(DEFAULT_MAX_RESULT_ROWS, pageSize), dbUser, null), ThreadPools.NETWORK_POOL)
                .thenCompose(submit -> {
                    notifyStatus(onStatus, submit);
                    return pollJobUntilDone(submit.getJobId(), onStatus);
                })
                .thenCompose(status -> {
                    if ("SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
                        return CompletableFuture.supplyAsync(() -> requestResultPaged(status.getJobId(), trimmed, false, pageSize, status), ThreadPools.NETWORK_POOL)
                                .thenAccept(res -> {
                                    notifyStatus(onStatus, status);
                                    if (onSuccess != null) {
                                        onSuccess.accept(res);
                                    }
                                });
                    }
                    RuntimeException failure = new RuntimeException(status.getMessage() != null
                            ? status.getMessage()
                            : ("任务" + status.getJobId() + " 状态 " + status.getStatus()));
                    if (onError != null) {
                        onError.accept(failure);
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .exceptionally(ex -> {
                    if (onError != null) {
                        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                        onError.accept(new RuntimeException(cause));
                    }
                    return null;
                });
    }

    public CompletableFuture<AsyncJobStatus> cancelJob(String jobId, String reason) {
        return CompletableFuture.supplyAsync(() -> doCancel(jobId, reason), ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<List<AsyncJobStatus>> listJobs() {
        return CompletableFuture.supplyAsync(this::doList, ThreadPools.NETWORK_POOL);
    }

    private AsyncJobStatus submitJob(String sql) {
        return submitJob(sql, determineMaxRows(DEFAULT_MAX_RESULT_ROWS, null), null, null);
    }

    private AsyncJobStatus submitJob(String sql, Integer maxResultRows, String dbUser, String label) {
        OperationLog.log("提交 /jobs/submit ... dbUser=" + (dbUser == null ? "<default>" : dbUser)
                + (maxResultRows != null && maxResultRows > 0 ? (" maxResultRows=" + maxResultRows) : "")
                + (label != null && !label.isBlank() ? (" label=" + label) : ""));
        AsyncJobStatus status = remoteClient.submitJob(sql, maxResultRows, dbUser, label);
        logStatus("已提交", status);
        return status;
    }



    private CompletableFuture<AsyncJobStatus> pollJobUntilDone(String jobId, Consumer<AsyncJobStatus> onStatus) {
        return CompletableFuture.supplyAsync(() -> {
            AsyncJobStatus latest = null;
            boolean finished = false;
            while (!finished) {
                latest = requestStatus(jobId);
                notifyStatus(onStatus, latest);
                finished = isTerminal(latest.getStatus());
                if (!finished) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("轮询中断", e);
                    }
                }
            }
            return latest;
        }, ThreadPools.NETWORK_POOL);
    }

    private AsyncJobStatus requestStatus(String jobId) {
        AsyncJobStatus status = remoteClient.pollStatus(jobId);
        logStatus("状态轮询", status);
        if (isTerminalFailure(status)) {
            throw new RuntimeException(status.getMessage() != null ? status.getMessage() : "任务失败或排队超时");
        }
        return status;
    }

    public SqlExecResult requestResultPaged(String jobId, String sql, boolean fetchAllPages, Integer pageSizeOverride) {
        return requestResultPaged(jobId, sql, fetchAllPages, pageSizeOverride, null);
    }

    public SqlExecResult requestResultPaged(String jobId, String sql, boolean fetchAllPages, Integer pageSizeOverride, AsyncJobStatus finalStatus) {
        int pageSize = resolvePageSize(pageSizeOverride);
        ResultFetchContext context = new ResultFetchContext(AsyncResultConfig.getPageBase(),
                AsyncResultConfig.getRetryMaxAttempts(),
                AsyncResultConfig.getRetryBaseDelayMs(),
                AsyncResultConfig.getRetryMaxDelayMs(),
                AsyncResultConfig.getRemoveAfterFetchStrategy());
        int pageIndex = 0; // zero-based index for client side
        SqlExecResult firstPage = requestResultPageWithRetry(jobId, sql, pageIndex, pageSize, context, finalStatus);
        if (!fetchAllPages || firstPage == null) {
            context.cleanupResultIfNeeded(jobId, pageSize);
            return firstPage;
        }

        List<List<String>> allRows = new ArrayList<>();
        List<java.util.Map<String, String>> allRowMaps = new ArrayList<>();
        if (firstPage.getRows() != null) {
            allRows.addAll(firstPage.getRows());
        }
        if (firstPage.getRowMaps() != null) {
            allRowMaps.addAll(firstPage.getRowMaps());
        }

        boolean hasNext = Boolean.TRUE.equals(firstPage.getHasNext());
        boolean truncated = Boolean.TRUE.equals(firstPage.getTruncated());
        String note = firstPage.getNote();
        boolean stoppedByLimit = false;

        while (hasNext && pageIndex + 1 < MAX_ACCUMULATED_PAGES && allRows.size() < MAX_ACCUMULATED_ROWS) {
            pageIndex++;
            SqlExecResult pageResult = requestResultPageWithRetry(jobId, sql, pageIndex, pageSize, context, finalStatus);
            if (pageResult.getRows() != null) {
                allRows.addAll(pageResult.getRows());
            }
            if (pageResult.getRowMaps() != null) {
                allRowMaps.addAll(pageResult.getRowMaps());
            }
            if (pageResult.getTruncated() != null && pageResult.getTruncated()) {
                truncated = true;
            }
            if (pageResult.getNote() != null && !pageResult.getNote().isBlank()) {
                note = pageResult.getNote();
            }
            hasNext = Boolean.TRUE.equals(pageResult.getHasNext());
            if (allRows.size() >= MAX_ACCUMULATED_ROWS) {
                stoppedByLimit = true;
                break;
            }
        }

        if (stoppedByLimit && hasNext) {
            OperationLog.log("[" + jobId + "] 达到分页累积上限，已停止继续拉取剩余数据");
        }

        context.cleanupResultIfNeeded(jobId, pageSize);
        return SqlExecResult.builder(sql)
                .columns(firstPage.getColumns())
                .columnDefs(firstPage.getColumnDefs())
                .rows(allRows)
                .rowMaps(allRowMaps)
                .rowCount(allRows.size())
                .success(true)
                .message(firstPage.getMessage())
                .jobId(jobId)
                .status(firstPage.getStatus())
                .progressPercent(firstPage.getProgressPercent())
                .elapsedMillis(firstPage.getElapsedMillis())
                .durationMillis(firstPage.getDurationMillis())
                .rowsAffected(firstPage.getRowsAffected())
                .returnedRowCount(firstPage.getReturnedRowCount())
                .actualRowCount(firstPage.getActualRowCount())
                .maxVisibleRows(firstPage.getMaxVisibleRows())
                .maxTotalRows(firstPage.getMaxTotalRows())
                .hasResultSet(firstPage.getHasResultSet())
                .page(pageIndex + context.resolvedBase())
                .pageSize(pageSize)
                .hasNext(hasNext)
                .truncated(truncated)
                .note(note)
                .queuedAt(firstPage.getQueuedAt())
                .queueDelayMillis(firstPage.getQueueDelayMillis())
                .overloaded(firstPage.getOverloaded())
                .threadPool(firstPage.getThreadPool())
                .commandTag(firstPage.getCommandTag())
                .updateCount(firstPage.getUpdateCount())
                .notices(firstPage.getNotices())
                .warnings(firstPage.getWarnings())
                .build();
    }

    private SqlExecResult requestResultPageWithRetry(String jobId,
                                                     String sql,
                                                     int pageIndex,
                                                     int pageSize,
                                                     ResultFetchContext context,
                                                     AsyncJobStatus finalStatus) {
        int finalPageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        int attempts = Math.max(1, context.maxAttempts());
        int attempt = 0;
        long delay = Math.max(0, context.baseDelayMs());
        boolean useAlternateBase = false;
        RuntimeException lastError = null;
        boolean transientDetected = false;
        while (attempt < attempts) {
            attempt++;
            int page = context.computePageNumber(pageIndex, useAlternateBase);
            try {
                ResultResponse resp = remoteClient.fetchResult(jobId, context.shouldUseRemoveAfterFetch(pageIndex), page, finalPageSize, null, null);
                boolean success = Boolean.TRUE.equals(resp.getSuccess());
                String respStatus = resp.getStatus();
                Integer returnedRowCount = resp.getReturnedRowCount();
                Integer actualRowCount = resp.getActualRowCount();
                Boolean hasResultSet = resp.getHasResultSet();
                String message = pickMessage(resp);
                logAttempt(jobId, attempt, pageIndex, page, finalPageSize, context, success, returnedRowCount, actualRowCount, hasResultSet, message, useAlternateBase);

                boolean expectResult = shouldExpectResult(finalStatus, hasResultSet, actualRowCount, returnedRowCount, respStatus);
                boolean transientUnavailable = isTransientUnavailable(message) || (!success && expectResult);
                SqlExecResult parsed = null;
                boolean contradictoryEmpty = false;
                if (success) {
                    parsed = parseResultResponse(resp, sql, jobId, page, pageSize);
                    contradictoryEmpty = isContradictoryEmpty(parsed, finalStatus, actualRowCount, returnedRowCount, hasResultSet);
                }

                if (!success && transientUnavailable) {
                    transientDetected = true;
                    lastError = new TransientResultUnavailableException(jobId, message != null ? message : "结果暂不可用");
                } else if (!success) {
                    throw new RuntimeException(message != null ? message : "结果已过期，请重新执行 SQL");
                }

                boolean shouldRetryForTransient = transientUnavailable;
                if (parsed != null && !contradictoryEmpty && !shouldRetryForTransient) {
                    context.resolveBase(page, pageIndex);
                    context.markResultFetched();
                    return parsed;
                }

                if (contradictoryEmpty || shouldRetryForTransient) {
                    if (context.canFallbackPageBase(useAlternateBase)) {
                        OperationLog.log("[" + jobId + "] 检测到结果为空/过期，将尝试切换 page 基准并重试");
                        useAlternateBase = true;
                    }
                    transientDetected = true;
                    lastError = new TransientResultUnavailableException(jobId, message != null ? message : "结果暂不可用");
                } else if (parsed == null) {
                    lastError = new RuntimeException(message != null ? message : "结果解析失败");
                }
            } catch (ResultNotReadyException rnre) {
                transientDetected = true;
                lastError = rnre;
            } catch (ResultExpiredException re) {
                lastError = re;
                break;
            } catch (RuntimeException ex) {
                lastError = ex;
            }

            if (attempt >= attempts) {
                break;
            }
            long sleepMs = Math.min(context.maxDelayMs(), delay);
            OperationLog.log("[" + jobId + "] /jobs/result attempt=" + attempt + " 将在 " + sleepMs + "ms 后重试");
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("拉取结果被中断", ie);
            }
            delay = Math.min(context.maxDelayMs(), delay * 2);
        }
        if (transientDetected) {
            if (lastError instanceof TransientResultUnavailableException) {
                throw lastError;
            }
            throw new TransientResultUnavailableException(jobId, lastError != null ? lastError.getMessage() : "结果暂不可用", lastError);
        }
        throw lastError != null ? lastError : new RuntimeException("结果获取失败");
    }

    SqlExecResult parseResultResponse(ResultResponse resp, String sql, String jobId, int requestedPage, int requestedPageSize) {
        try {
            boolean success = Boolean.TRUE.equals(resp.getSuccess());
            String status = resp.getStatus() != null ? resp.getStatus() : "";
            Integer progress = resp.getProgressPercent();
            Long duration = resp.getDurationMillis();
            Integer rowsAffected = resp.getRowsAffected();
            Integer returnedRowCount = resp.getReturnedRowCount();
            Integer actualRowCount = resp.getActualRowCount();
            Integer maxVisibleRows = resp.getMaxVisibleRows();
            Integer maxTotalRows = resp.getMaxTotalRows();
            Boolean hasResultSet = resp.getHasResultSet();
            Boolean isSelect = resp.getIsSelect();
            String resultType = resp.getResultType();
            Integer updateCount = resp.getUpdateCount();
            String commandTag = resp.getCommandTag();
            Boolean hasNext = resp.getHasNext() != null ? resp.getHasNext() : false;
            Boolean truncated = resp.getTruncated();
            Integer respPage = resp.getPage() != null ? resp.getPage() : requestedPage;
            Integer respPageSize = resp.getPageSize() != null ? resp.getPageSize() : requestedPageSize;
            String note = resp.getNote();
            Long queuedAt = resp.getQueuedAt();
            Long queueDelayMillis = resp.getQueueDelayMillis();
            Boolean overloaded = resp.getOverloaded();
            ThreadPoolSnapshot threadPool = resp.getThreadPool();
            String message = pickMessage(resp);
            List<String> notices = resp.getNotices();
            List<String> warnings = resp.getWarnings();

            Integer affectedRows = resolveAffectedRows(rowsAffected, updateCount, commandTag);
            boolean finalHasResultSet = determineHasResultSet(sql, hasResultSet, isSelect, resultType);

            if (!success) {
                OperationLog.log("[" + jobId + "] 任务失败/未完成: " + message);
                throw new RuntimeException(message != null ? message : "结果已过期，请重新执行 SQL");
            }

            List<String> columns = resp.getColumns() != null ? resp.getColumns() : List.of();
            List<java.util.Map<String, String>> rowMaps = resp.getRowMaps() != null ? resp.getRowMaps() : List.of();
            List<List<String>> rows = extractRows(rowMaps, columns);
            int rowCount = returnedRowCount != null ? returnedRowCount : rows.size();

            logResultDetails(jobId, respPage, respPageSize, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows,
                    hasNext, truncated, note, columns, resp.getRawRows(), rows, queuedAt, queueDelayMillis, overloaded, threadPool);

            SqlExecResult.Builder builder = SqlExecResult.builder(sql)
                    .columns(columns)
                    .rows(rows)
                    .rowMaps(rowMaps)
                    .rowCount(rowCount)
                    .success(true)
                    .message(message)
                    .jobId(jobId)
                    .status(status)
                    .progressPercent(progress)
                    .durationMillis(duration)
                    .rowsAffected(affectedRows)
                    .returnedRowCount(returnedRowCount)
                    .actualRowCount(actualRowCount)
                    .maxVisibleRows(maxVisibleRows)
                    .maxTotalRows(maxTotalRows)
                    .page(respPage)
                    .pageSize(respPageSize)
                    .hasNext(hasNext)
                    .truncated(truncated)
                    .note(note)
                    .queuedAt(queuedAt)
                    .queueDelayMillis(queueDelayMillis)
                    .overloaded(overloaded)
                    .threadPool(threadPool)
                    .commandTag(commandTag)
                    .updateCount(updateCount)
                    .notices(notices)
                    .warnings(warnings);

            if (!finalHasResultSet) {
                return builder
                        .columns(List.of())
                        .rows(List.of())
                        .rowMaps(List.of())
                        .rowCount(0)
                        .hasResultSet(false)
                        .build();
            }

            return builder.hasResultSet(finalHasResultSet).build();
        } catch (Exception e) {
            OperationLog.log("[" + jobId + "] 解析 /jobs/result 失败: " + e.getMessage());
            throw new RuntimeException("解析结果失败: " + e.getMessage(), e);
        }
    }

    private Integer resolveAffectedRows(Integer rowsAffected, Integer updateCount, String commandTag) {
        Integer candidate = firstNonNegative(rowsAffected, updateCount);
        if (candidate != null) {
            return candidate;
        }
        java.util.OptionalInt parsed = CommandTagParser.parseAffectedRows(commandTag);
        return parsed.isPresent() ? parsed.getAsInt() : null;
    }


    private boolean determineHasResultSet(String sql, Boolean respHasResultSet, Boolean isSelect, String resultType) {
        if (respHasResultSet != null) {
            return respHasResultSet;
        }
        if (isSelect != null) {
            return isSelect;
        }
        if (resultType != null) {
            if ("RESULT_SET".equalsIgnoreCase(resultType)) {
                return true;
            }
            if ("NON_QUERY".equalsIgnoreCase(resultType)) {
                return false;
            }
        }
        return SqlTopLevelClassifier.classify(sql) == SqlTopLevelClassifier.TopLevelType.RESULT_SET;
    }

    private Integer firstNonNegative(Integer... values) {
        if (values == null) return null;
        for (Integer v : values) {
            if (v != null && v >= 0) {
                return v;
            }
        }
        return null;
    }

    private int resolvePageSize(Integer desired) {
        int fallback = Config.getResultPageSizeOrDefault(DEFAULT_PAGE_SIZE);
        int target = desired == null ? fallback : desired;
        if (target < 1) {
            OperationLog.log("pageSize=" + target + " 无效，使用默认值 " + fallback);
            target = fallback;
        }
        if (target > MAX_PAGE_SIZE) {
            OperationLog.log("pageSize=" + target + " 超出上限，已裁剪到 " + MAX_PAGE_SIZE);
            target = MAX_PAGE_SIZE;
        }
        return target;
    }

    private int determineMaxRows(Integer explicitMaxRows, Integer pageSizeOverride) {
        int targetPageSize = resolvePageSize(pageSizeOverride);
        if (explicitMaxRows != null && explicitMaxRows > 0) {
            return Math.max(explicitMaxRows, targetPageSize);
        }
        return targetPageSize;
    }

    private List<List<String>> extractRows(List<java.util.Map<String, String>> rowMaps, List<String> columns) {
        List<List<String>> rows = new ArrayList<>();
        if (rowMaps == null) {
            return rows;
        }
        List<String> targetColumns = columns == null ? List.of() : columns;
        for (java.util.Map<String, String> map : rowMaps) {
            List<String> row = new ArrayList<>();
            for (String col : targetColumns) {
                row.add(map != null ? map.get(col) : null);
            }
            rows.add(row);
        }
        return rows;
    }

    private void logResultDetails(String jobId, Integer respPage, Integer respPageSize, Integer returnedRowCount,
                                  Integer actualRowCount, Integer maxVisibleRows, Integer maxTotalRows, Boolean hasNext,
                                  Boolean truncated, String note, List<String> columns, JsonArray rowsJson, List<List<String>> rows,
                                  Long queuedAt, Long queueDelayMillis, Boolean overloaded, ThreadPoolSnapshot threadPool) {
        OperationLog.log("[" + jobId + "] 任务完成，page=" + respPage + " pageSize=" + respPageSize
                + "，returnedRowCount=" + (returnedRowCount != null ? returnedRowCount : rows.size())
                + (actualRowCount != null ? ("，actualRowCount=" + actualRowCount) : "")
                + (maxVisibleRows != null ? ("，maxVisibleRows=" + maxVisibleRows) : "")
                + (maxTotalRows != null ? ("，maxTotalRows=" + maxTotalRows) : "")
                + (hasNext != null && hasNext ? "，hasNext=true" : "")
                + (truncated != null && truncated ? "，truncated=true" : "")
                + (queuedAt != null ? ("，queuedAt=" + queuedAt) : "")
                + (queueDelayMillis != null ? ("，queueDelayMillis=" + queueDelayMillis) : "")
                + (overloaded != null ? ("，overloaded=" + overloaded) : ""));
        OperationLog.log("[" + jobId + "] resultRows.size=" + (rowsJson == null ? 0 : rowsJson.size())
                + " columns=" + columns.size() + " -> " + abbreviate(String.join(",", columns)));
        if (rowsJson != null && rowsJson.size() > 0 && rowsJson.get(0).isJsonObject()) {
            var firstKeys = new ArrayList<String>();
            rowsJson.get(0).getAsJsonObject().keySet().forEach(firstKeys::add);
            OperationLog.log("[" + jobId + "] first row keys=" + abbreviate(String.join(",", firstKeys)));
        }
        if (threadPool != null) {
            OperationLog.log("[" + jobId + "] threadPool=" + threadPool.toString());
        }
        if (note != null && !note.isBlank()) {
            OperationLog.log("[" + jobId + "] note: " + note);
        }
        if (Boolean.TRUE.equals(truncated)) {
            OperationLog.log("[" + jobId + "] 后端返回结果被截断（truncated=true）");
        }
    }

    private void logAttempt(String jobId, int attempt, int pageIndex, int page, int pageSize, ResultFetchContext context,
                            boolean success, Integer returnedRowCount, Integer actualRowCount, Boolean hasResultSet,
                            String message, boolean alternateBase) {
        OperationLog.log("[" + jobId + "] /jobs/result attempt=" + attempt
                + " page=" + page
                + " (base=" + context.describeBase(page, alternateBase) + ")"
                + " pageSize=" + pageSize
                + " removeAfterFetch=" + context.shouldUseRemoveAfterFetch(pageIndex)
                + " success=" + success
                + (hasResultSet != null ? (" hasResultSet=" + hasResultSet) : "")
                + (returnedRowCount != null ? (" returnedRowCount=" + returnedRowCount) : "")
                + (actualRowCount != null ? (" actualRowCount=" + actualRowCount) : "")
                + (message != null && !message.isBlank() ? (" message=" + abbreviate(message)) : ""));
    }

    private boolean isTransientUnavailable(String message) {
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("result expired") || lower.contains("not available");
    }

    private boolean isContradictoryEmpty(SqlExecResult parsed,
                                         AsyncJobStatus finalStatus,
                                         Integer respActualRowCount,
                                         Integer returnedRowCount,
                                         Boolean respHasResultSet) {
        boolean hasResultSet = coalesceHasResultSet(parsed, finalStatus, respHasResultSet);
        if (!hasResultSet) {
            return false;
        }
        int expectedActual = firstPositive(respActualRowCount, parsed.getActualRowCount(),
                finalStatus != null ? finalStatus.getActualRowCount() : null,
                finalStatus != null ? finalStatus.getReturnedRowCount() : null,
                returnedRowCount);
        int returned = firstPositive(parsed.getReturnedRowCount(), returnedRowCount);
        int realRows = parsed.getRows() != null ? parsed.getRows().size() : 0;
        return expectedActual > 0 && (returned == 0 || realRows == 0);
    }

    private boolean coalesceHasResultSet(SqlExecResult parsed, AsyncJobStatus finalStatus, Boolean respHasResultSet) {
        if (parsed.getHasResultSet() != null) {
            return parsed.getHasResultSet();
        }
        if (respHasResultSet != null) {
            return respHasResultSet;
        }
        if (finalStatus != null && finalStatus.getHasResultSet() != null) {
            return finalStatus.getHasResultSet();
        }
        return true;
    }

    private int firstPositive(Integer... values) {
        if (values == null) {
            return 0;
        }
        for (Integer v : values) {
            if (v != null && v > 0) {
                return v;
            }
        }
        return 0;
    }

    private boolean shouldExpectResult(AsyncJobStatus finalStatus,
                                       Boolean respHasResultSet,
                                       Integer respActualRowCount,
                                       Integer returnedRowCount,
                                       String respStatus) {
        boolean statusSucceeded = respStatus != null && "SUCCEEDED".equalsIgnoreCase(respStatus);
        if (finalStatus != null && "SUCCEEDED".equalsIgnoreCase(finalStatus.getStatus())) {
            statusSucceeded = true;
        }
        boolean hasResult = Boolean.TRUE.equals(respHasResultSet)
                || (finalStatus != null && Boolean.TRUE.equals(finalStatus.getHasResultSet()));
        int expectedRows = firstPositive(respActualRowCount,
                returnedRowCount,
                finalStatus != null ? finalStatus.getActualRowCount() : null,
                finalStatus != null ? finalStatus.getReturnedRowCount() : null);
        return statusSucceeded && hasResult && expectedRows > 0;
    }

    private final class ResultFetchContext {
        private final AsyncResultConfig.PageBase configuredBase;
        private final int maxAttempts;
        private final long baseDelayMs;
        private final long maxDelayMs;
        private final AsyncResultConfig.RemoveAfterFetchStrategy removeAfterFetchStrategy;
        private Integer resolvedBase;
        private boolean fetched;

        ResultFetchContext(AsyncResultConfig.PageBase configuredBase,
                           int maxAttempts,
                           long baseDelayMs,
                           long maxDelayMs,
                           AsyncResultConfig.RemoveAfterFetchStrategy removeAfterFetchStrategy) {
            this.configuredBase = configuredBase == null ? AsyncResultConfig.PageBase.AUTO : configuredBase;
            this.maxAttempts = maxAttempts <= 0 ? 1 : maxAttempts;
            this.baseDelayMs = Math.max(0, baseDelayMs);
            this.maxDelayMs = Math.max(this.baseDelayMs, maxDelayMs);
            this.removeAfterFetchStrategy = removeAfterFetchStrategy == null
                    ? AsyncResultConfig.RemoveAfterFetchStrategy.AUTO
                    : removeAfterFetchStrategy;
        }

        int computePageNumber(int pageIndex, boolean useAlternateBase) {
            int base = resolvedBase != null ? resolvedBase : determineBase(useAlternateBase);
            return pageIndex + base;
        }

        int resolvedBase() {
            return resolvedBase != null ? resolvedBase : determineBase(false);
        }

        void resolveBase(int pageNumber, int pageIndex) {
            this.resolvedBase = Math.max(0, pageNumber - pageIndex);
        }

        boolean shouldUseRemoveAfterFetch(int pageIndex) {
            if (removeAfterFetchStrategy == AsyncResultConfig.RemoveAfterFetchStrategy.TRUE) {
                return true;
            }
            // 避免首次拉取后立即清理导致无法重试，AUTO/false 统一使用 false
            return false;
        }

        boolean canFallbackPageBase(boolean alreadyAlternate) {
            return configuredBase == AsyncResultConfig.PageBase.AUTO && resolvedBase == null && !alreadyAlternate;
        }

        int maxAttempts() {
            return maxAttempts;
        }

        long baseDelayMs() {
            return baseDelayMs;
        }

        long maxDelayMs() {
            return maxDelayMs;
        }

        void markResultFetched() {
            this.fetched = true;
        }

        void cleanupResultIfNeeded(String jobId, int pageSize) {
            if (!fetched) {
                return;
            }
            if (removeAfterFetchStrategy == AsyncResultConfig.RemoveAfterFetchStrategy.FALSE) {
                return;
            }
            if (removeAfterFetchStrategy == AsyncResultConfig.RemoveAfterFetchStrategy.AUTO) {
                return; // 默认不强制清理，避免二次拉取导致副作用
            }
            int page = computePageNumber(0, false);
            try {
                remoteClient.fetchResult(jobId, true, page, pageSize, null, null);
                OperationLog.log("[" + jobId + "] 已使用 removeAfterFetch=true 清理结果缓存 (page=" + page + ")");
            } catch (Exception e) {
                OperationLog.log("[" + jobId + "] 清理结果缓存失败: " + e.getMessage());
            }
        }

        String describeBase(int page, boolean alternateBase) {
            int base = resolvedBase != null ? resolvedBase : determineBase(alternateBase);
            return base == 0 ? "0-based" : "1-based";
        }

        private int determineBase(boolean useAlternateBase) {
            int preferred = switch (configuredBase) {
                case ZERO_BASED -> 0;
                case ONE_BASED -> 1;
                default -> 1;
            };
            if (useAlternateBase) {
                return preferred == 0 ? 1 : 0;
            }
            return preferred;
        }
    }

    private AsyncJobStatus doCancel(String jobId, String reason) {
        AsyncJobStatus status = remoteClient.cancelJob(jobId, reason);
        OperationLog.log("[" + jobId + "] 取消请求已发送，状态 " + status.getStatus());
        return status;
    }

    private List<AsyncJobStatus> doList() {
        return remoteClient.listActiveJobs();
    }

    private String pickMessage(ResultResponse resp) {
        if (resp == null) {
            return null;
        }
        String error = trimToNull(resp.getErrorMessage());
        String message = trimToNull(resp.getMessage());
        if (error != null) {
            return error;
        }
        if (message != null) {
            return message;
        }
        return trimToNull(resp.getNote());
    }

    private String trimToNull(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private boolean isTerminal(String status) {
        if (status == null) return true;
        String s = status.toUpperCase();
        return "SUCCEEDED".equals(s) || "FAILED".equals(s) || "CANCELLED".equals(s);
    }

    private boolean isTerminalFailure(AsyncJobStatus status) {
        if (status == null) {
            return false;
        }
        if (Boolean.FALSE.equals(status.getSuccess()) && "FAILED".equalsIgnoreCase(status.getStatus())) {
            return true;
        }
        if ("FAILED".equalsIgnoreCase(status.getStatus())) {
            String msg = status.getMessage();
            if (status.getOverloaded() != null && status.getOverloaded()) {
                return true;
            }
            if (msg != null && msg.toLowerCase().contains("queue delay")) {
                return true;
            }
        }
        return false;
    }

    private void notifyStatus(Consumer<AsyncJobStatus> onStatus, AsyncJobStatus status) {
        if (onStatus != null && status != null) {
            onStatus.accept(status);
        }
    }

    private void logStatus(String scene, AsyncJobStatus status) {
        if (status == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(status.getJobId()).append("] ").append(scene)
                .append(" status=").append(status.getStatus());
        if (status.getProgressPercent() != null) {
            sb.append(" progress=").append(status.getProgressPercent()).append("%");
        }
        if (status.getQueuedAt() != null) {
            sb.append(" queuedAt=").append(status.getQueuedAt());
        }
        if (status.getQueueDelayMillis() != null) {
            sb.append(" queueDelayMillis=").append(status.getQueueDelayMillis());
        }
        if (status.getOverloaded() != null) {
            sb.append(" overloaded=").append(status.getOverloaded());
        }
        if (status.getThreadPool() != null) {
            sb.append(" threadPool=[").append(status.getThreadPool().toString()).append("]");
        }
        if (status.getMessage() != null && !status.getMessage().isBlank()) {
            sb.append(" message=").append(abbreviate(status.getMessage()));
        }
        OperationLog.log(sb.toString());
    }

    private String abbreviate(String text) {
        if (text == null) {
            return "";
        }
        String t = text.strip();
        return t.length() > 600 ? t.substring(0, 600) + "..." : t;
    }
}
