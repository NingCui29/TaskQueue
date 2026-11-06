package com.cn.tasks

import com.cn.tasks.log.TaskLog
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * 任务单元。
 *
 * 封装一段可挂起的执行逻辑，附带优先级、超时、进度回报与生命周期回调。
 *
 * ```kotlin
 * val task = task<String> {
 *     name = "LoadData"
 *     priority = TaskPriority.HIGH
 *     timeout = 30.seconds
 *     block { progress -> fetchData(progress) }
 *     onSuccess { render(it) }
 *     onError { showError(it) }
 * }
 * queue.enqueue(task)
 * ```
 *
 * ## 相对旧 `com.cn.core.task.Task` 的调整
 *
 * | # | 旧实现问题 | 影响 | 本实现 |
 * |---|-----------|------|--------|
 * | 1 | `data class` 却又手写 `equals`/`hashCode` | 自动生成的结构化相等被覆盖，`copy()` 还会丢失 `sequence`/`isCancelled`/当前状态等可变运行期状态，复制出一个"半初始化"的任务 | 改为普通 `class`，仅保留基于 [id] 的相等语义 |
 * | 2 | `sequence`/`isCancelled`/`isTimeout` 为裸 `var` | 入队线程写、执行线程读，无可见性保证 | `sequence`/`isTimeout` 加 `@Volatile`，取消标志改用 [AtomicBoolean] |
 * | 3 | `onSuccess` 在 `try` 内调用 | 回调自身抛异常会被当作任务失败，`onError` 随即被触发，一次执行同时走成功与失败两条路径 | 结果先落定，回调统一在 [runCallback] 中隔离执行 |
 * | 4 | 回调异常无隔离 | 任一回调抛异常即污染任务结果 | [runCallback] 捕获并降级为一条 error 日志 |
 * | 5 | 捕获 [CancellationException] 后物化为 `Result.failure` | 吞掉取消信号，外层协程无法感知自身已被取消，违反结构化并发 | 取消一律向上重抛，见下方"取消语义" |
 * | 6 | 取消路径未触发 `onError` 却触发 `onComplete`，成功路径 `onComplete` 在 `finally` 中 | 回调时序依赖 `finally`，可读性差 | 三类终态的回调顺序显式固定 |
 *
 * ## 取消语义
 *
 * [execute] 的返回值只承载"任务自身的成败"：
 * - 正常返回 → `Result.success`
 * - 业务异常 → `Result.failure(异常)`
 * - 超时 → `Result.failure(TaskTimeoutException)`，同时 [isTimeout] 置真
 * - **取消 → 抛出 [CancellationException]，不进入 `Result`**
 *
 * 取消属于协程作用域事件而非任务结果，重抛才能让承载它的 `Job` 正确进入取消态。
 * 调用方需自行 `try/catch`，队列实现已按此契约处理。
 *
 * 队列已接纳的任务在结束时通知一次 [onComplete]，包括执行前取消或被淘汰。
 * 执行中取消会等待任务体的清理退出；未接纳的拒绝以及 [SerialTaskQueue.clear] 不通知。
 * 同一实例只能有一个在途提交；复用须等待上一轮执行及回调全部退出，不能跨队列同时提交。
 *
 * @param T 执行结果类型
 * @param id 唯一标识，默认随机 UUID
 * @param name 任务名，用于日志与调试
 * @param priority 优先级，默认 [TaskPriority.NORMAL]
 * @param timeout 超时时长，默认 [Duration.INFINITE] 表示不限时
 * @param block 任务体，入参为 [ProgressReporter]
 * @param onSuccess 成功回调
 * @param onError 失败回调（含超时；不含取消）
 * @param onComplete 已接纳任务的结束回调，成功、失败、取消均触发一次；执行中取消等待清理结束
 * @param onStateChange 状态变化回调
 * @param onTimeout 本次执行的配置超时回调，触发时机早于 [onError]；下游超时异常只交付 [onError]
 *
 * @see TaskBuilder
 * @see task
 * @see TaskState
 */
class Task<T>(
    val id: String = UUID.randomUUID().toString(),
    val name: String = "Task-$id",
    val priority: TaskPriority = TaskPriority.NORMAL,
    val timeout: Duration = Duration.INFINITE,
    val block: suspend (ProgressReporter) -> T,
    val onSuccess: ((T) -> Unit)? = null,
    val onError: ((Throwable) -> Unit)? = null,
    val onComplete: (() -> Unit)? = null,
    val onStateChange: ((TaskState) -> Unit)? = null,
    val onTimeout: (() -> Unit)? = null,
) {

    /**
     * 入队序号。
     *
     * 同优先级任务据此保持 FIFO。由队列在入队时赋值，调用方无需关心。
     */
    @Volatile
    var sequence: Long = 0L
        internal set

    private val cancelled = AtomicBoolean(false)

    /**
     * 是否已被取消。
     *
     * 置真后，尚未开始的任务不会执行 [block]；已在执行的任务由队列取消其 `Job`。
     */
    val isCancelled: Boolean
        get() = cancelled.get()

    /** 是否因超过 [timeout] 而终止。 */
    @Volatile
    var isTimeout: Boolean = false
        private set

    private val stateLock = Any()

    private var _currentState: TaskState = TaskState.Pending()

    private var pendingStateVersion = 0L

    /** 每次接纳独立持有通知门闩，旧回调退出不能覆盖新一轮的完成记录。由 stateLock 保护。 */
    private class CompletionAttempt {
        var executionStarted = false
        var completionClaimed = false
    }

    private var completionAttempt = CompletionAttempt()

    /** 在队列锁内接纳任务时调用，不执行外部回调。 */
    internal fun prepareForEnqueue() {
        synchronized(stateLock) {
            completionAttempt = CompletionAttempt()
            pendingStateVersion++
            if (!cancelled.get()) _currentState = TaskState.Pending()
        }
    }

    /** 当前状态快照。 */
    val currentState: TaskState
        get() = synchronized(stateLock) { _currentState }

    /** 是否配置了有效超时。`Duration.INFINITE` 与非正值均视为不限时。 */
    val hasTimeout: Boolean
        get() = timeout != Duration.INFINITE && timeout.isPositive()

    private suspend fun runBlockWithProgress(): T {
        val executionContext = currentCoroutineContext()
        val callbackContext = executionContext[QueueExecutionContext]
        var acceptingProgress = true
        // 每次任务体独立持有报告器；体退出、协程取消或任务终止后均不再接收报告。
        val reporter = object : ProgressReporter {
            override fun report(progress: Float, message: String?) {
                if (progress.isNaN()) return
                val next = TaskState.Running(progress.coerceIn(0f, 1f), message)
                synchronized(stateLock) {
                    if (!acceptingProgress || !executionContext.isActive || cancelled.get() ||
                        _currentState !is TaskState.Running
                    ) return
                    _currentState = next
                }
                val notifyChange = { runCallback("onStateChange") { onStateChange?.invoke(next) } }
                // 外部线程没有协程的线程标记；仍在原报告线程派发，并在退出时恢复它的标记。
                if (callbackContext == null) notifyChange() else callbackContext.notify(notifyChange)
            }
        }
        return try {
            block(reporter)
        } finally {
            synchronized(stateLock) { acceptingProgress = false }
        }
    }

    /**
     * 更新状态并派发 [onStateChange]。
     *
     * 赋值在锁内完成，回调在锁外触发——回调是外部代码，在持锁期间执行会把
     * 锁的持有时间交给调用方控制，并可能与队列锁形成环路。
     * 代价是高频并发更新下，回调到达顺序与 [currentState] 可能存在瞬时不一致；
     * 该类型的状态回调用于 UI 提示，不作为一致性依据。
     */
    internal fun updateState(newState: TaskState) {
        synchronized(stateLock) {
            // 在途所有权会保留到清理完成；取消通知重入时不重复派发相同终态。
            if (newState === TaskState.Cancelled && _currentState === TaskState.Cancelled) return
            // 队列位次来自锁外通知的快照；旧快照不能把已开始/已终止的任务退回等待态。
            if (newState is TaskState.Pending &&
                (cancelled.get() || _currentState !is TaskState.Pending)
            ) return
            _currentState = newState
        }
        runCallback("onStateChange") { onStateChange?.invoke(newState) }
    }

    /**
     * 为等待状态预留版本，返回可在队列锁外执行的发布动作。
     *
     * 队列必须在同一次持锁快照中为所有等待任务准备动作。通知期间重入生成的新快照
     * 会使旧动作失效，避免旧位次和队列大小覆盖较新的值；任务已开始或取消时也不再发布。
     */
    internal fun preparePendingState(newState: TaskState.Pending): () -> Unit {
        val version = synchronized(stateLock) { ++pendingStateVersion }
        return publish@{
            synchronized(stateLock) {
                if (version != pendingStateVersion || cancelled.get() ||
                    _currentState !is TaskState.Pending
                ) return@publish
                _currentState = newState
            }
            runCallback("onStateChange") { onStateChange?.invoke(newState) }
        }
    }

    /**
     * 执行任务。
     *
     * 由队列调用。执行流程：
     * 1. 已取消 → 置 [TaskState.Cancelled]，触发 `onComplete` 后抛出 [CancellationException]
     * 2. 置 [TaskState.Running]，执行 [block]（按需包裹 `withTimeout`）
     * 3. 结果落定后依次触发 `onTimeout` → `onSuccess`/`onError`，然后置 [TaskState.Completed]
     * 4. finally 触发一次 `onComplete`，终态回调抛出取消时也完成退出通知
     * 5. 执行与退出同时失败时致命错误优先，同等级保留原始错误，另一错误作为附加异常
     *
     * @return 成功值或失败异常
     * @throws CancellationException 任务被取消，或承载协程被取消
     */
    internal suspend fun execute(): Result<T> {
        val attempt = synchronized(stateLock) {
            val previous = completionAttempt
            if (previous.completionClaimed && !previous.executionStarted) {
                // 执行前取消已交付结束通知，迟到的派发不能再次执行或通知。
                throw CancellationException("Task '$name' was cancelled before execution")
            }
            // execute 也用于内部直接执行；保留同一任务顺序执行多次的行为。
            val next = if (previous.completionClaimed) CompletionAttempt().also { completionAttempt = it } else previous
            next.executionStarted = true
            next
        }
        var executionFailure: Throwable? = null
        try {
            return executeCore()
        } catch (error: Throwable) {
            executionFailure = error
            throw error
        } finally {
            // 退出通知仍只执行一次；其失败不能覆盖正在传播的致命错误。
            try {
                completeOnce(attempt)
            } catch (completionFailure: Throwable) {
                throw executionFailure?.let { combineTaskFailures(it, completionFailure) } ?: completionFailure
            }
        }
    }

    /**
     * 终结已接纳任务的取消。执行与取消在同一锁内决定结束通知归属，回调全部在锁外。
     * 已开始的任务由 execute.finally 通知；尚未开始的任务即使状态回调失败也通知完成。
     */
    internal fun notifyCancellation() {
        val notifyCompletion = synchronized(stateLock) {
            cancelled.set(true)
            val attempt = completionAttempt
            if (!attempt.executionStarted && !attempt.completionClaimed) {
                attempt.completionClaimed = true
                true
            } else false
        }
        var notificationFailure: Throwable? = null
        try {
            updateState(TaskState.Cancelled)
        } catch (failure: Throwable) {
            notificationFailure = failure
            throw failure
        } finally {
            if (notifyCompletion) {
                try {
                    runCallback("onComplete") { onComplete?.invoke() }
                } catch (completionFailure: Throwable) {
                    throw notificationFailure?.let { combineTaskFailures(it, completionFailure) } ?: completionFailure
                }
            }
        }
    }

    private fun completeOnce(attempt: CompletionAttempt) {
        val claimed = synchronized(stateLock) {
            if (attempt.completionClaimed) false else {
                attempt.completionClaimed = true
                true
            }
        }
        if (claimed) runCallback("onComplete") { onComplete?.invoke() }
    }

    /** 执行业务及结果通知，保留取消状态回调的既有异常传播顺序。 */
    private suspend fun executeCore(): Result<T> {
        var configuredTimeout: TaskTimeoutException? = null
        try {
            if (isCancelled) throw CancellationException("Task '$name' was cancelled before execution")

            val result: Result<T> = try {
                updateState(TaskState.Running())
                // Running 回调可以同步取消任务/队列，普通非挂起 block 也不能越过此次取消。
                currentCoroutineContext().ensureActive()
                if (isCancelled) throw CancellationException("Task '$name' was cancelled before its block")
                val outcome: Result<T> = if (hasTimeout) {
                    // 仅将本层超时转为任务失败；外层和业务内部的超时仍是取消。
                    withTimeoutOrNull(timeout) { Result.success(runBlockWithProgress()) } ?: run {
                        isTimeout = true
                        val failure = TaskTimeoutException(name, timeout)
                        configuredTimeout = failure
                        Result.failure(failure)
                    }
                } else {
                    Result.success(runBlockWithProgress())
                }
                currentCoroutineContext().ensureActive()
                outcome
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // 不合作的任务可能在取消后才报错，不再把旧错误交付给业务回调。
                currentCoroutineContext().ensureActive()
                Result.failure(e)
            }

            result.fold(
                onSuccess = { value ->
                    runCallback("onSuccess") { onSuccess?.invoke(value) }
                    currentCoroutineContext().ensureActive()
                    updateState(TaskState.Completed(success = true))
                },
                onFailure = { error ->
                    if (error === configuredTimeout) {
                        runCallback("onTimeout") { onTimeout?.invoke() }
                        currentCoroutineContext().ensureActive()
                    }
                    runCallback("onError") { onError?.invoke(error) }
                    currentCoroutineContext().ensureActive()
                    updateState(TaskState.Completed(success = false, error = error))
                },
            )
            return result
        } catch (e: CancellationException) {
            cancel()
            updateState(TaskState.Cancelled)
            throw e
        }
    }

    /**
     * 标记任务取消。
     *
     * 尚未开始的任务将跳过 [block]；已在执行的任务需由队列取消其 `Job` 才会真正中断，
     * 单独调用本方法不会打断正在运行的 [block]。
     */
    fun cancel() {
        synchronized(stateLock) {
            cancelled.set(true)
        }
    }

    /**
     * 执行一个外部回调，并将其异常隔离在任务结果之外。
     *
     * [CancellationException] 例外——它代表协程取消，必须继续向上传播。
     */
    private inline fun runCallback(which: String, action: () -> Unit) {
        try {
            action()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            TaskLog.e("Task '$name' $which callback threw", TAG, e)
        }
    }

    /** 基于 [id] 判等。 */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Task<*>) return false
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String =
        "Task(id=$id, name=$name, priority=$priority, state=$currentState)"

    private companion object {
        const val TAG = "Task"
    }
}

/**
 * 任务超时异常。
 *
 * ## 相对旧 `com.cn.core.task.TaskTimeoutException` 的调整
 *
 * 基类由 `Exception` 改为 `RuntimeException`，与 [TaskQueueFullException] 保持一致：
 * 超时是运行期条件，不应强制 Java 调用方声明 `throws`。
 *
 * @param taskName 超时任务名
 * @param timeout 配置的超时时长
 */
class TaskTimeoutException(
    val taskName: String,
    val timeout: Duration,
) : RuntimeException("Task '$taskName' timed out after $timeout")

/**
 * [Task] 构建器。
 *
 * 既支持链式调用，也支持 [task] DSL 的属性赋值写法。
 *
 * ```kotlin
 * val t = TaskBuilder<String>()
 *     .name("LoadData")
 *     .priority(TaskPriority.HIGH)
 *     .timeout(30.seconds)
 *     .block { progress -> fetchData(progress) }
 *     .onSuccess(::render)
 *     .build()
 * ```
 *
 * ## 相对旧 `com.cn.core.task.TaskBuilder` 的调整
 *
 * 1. 新增 [callback]，把 [TaskCallback] 接入构建链路。旧实现中 `TaskCallback` 与
 *    `TaskCallbackAdapter` 定义了却无任何生产代码引用，是一组死 API。
 * 2. `name` 由"构造即固化"改为惰性求值。旧实现在字段初始化时就把 `name` 定死成
 *    `"Task-$id"`，此后再调 `id(...)` 只改 id、不改 name，二者失配；
 *    现仅在未显式设置 name 时才按最终 id 生成。
 *
 * @param T 执行结果类型
 */
class TaskBuilder<T> {

    /** 任务 id，默认随机 UUID。 */
    var id: String = UUID.randomUUID().toString()

    /** 任务名。未显式设置时按最终 [id] 生成为 `Task-<id>`。 */
    var name: String? = null

    /** 优先级。 */
    var priority: TaskPriority = TaskPriority.NORMAL

    /** 超时时长。 */
    var timeout: Duration = Duration.INFINITE

    /** 任务体。构建前必须设置。 */
    var block: (suspend (ProgressReporter) -> T)? = null

    /** 成功回调。 */
    var onSuccess: ((T) -> Unit)? = null

    /** 失败回调。 */
    var onError: ((Throwable) -> Unit)? = null

    /** 终态回调。 */
    var onComplete: (() -> Unit)? = null

    /** 状态变化回调。 */
    var onStateChange: ((TaskState) -> Unit)? = null

    /** 超时回调。 */
    var onTimeout: (() -> Unit)? = null

    /** 设置任务 id。 */
    fun id(id: String) = apply { this.id = id }

    /** 设置任务名。 */
    fun name(name: String) = apply { this.name = name }

    /** 设置优先级。 */
    fun priority(priority: TaskPriority) = apply { this.priority = priority }

    /** 设置超时时长。 */
    fun timeout(timeout: Duration) = apply { this.timeout = timeout }

    /** 设置超时时长，单位毫秒。 */
    fun timeout(timeoutMillis: Long) = apply { this.timeout = timeoutMillis.milliseconds }

    /** 设置任务体（可回报进度）。 */
    fun block(block: suspend (ProgressReporter) -> T) = apply { this.block = block }

    /** 设置任务体（无需回报进度）。 */
    fun block(block: suspend () -> T) = apply {
        this.block = { _: ProgressReporter -> block() }
    }

    /** 设置成功回调。 */
    fun onSuccess(callback: (T) -> Unit) = apply { this.onSuccess = callback }

    /** 设置失败回调。 */
    fun onError(callback: (Throwable) -> Unit) = apply { this.onError = callback }

    /** 设置终态回调。 */
    fun onComplete(callback: () -> Unit) = apply { this.onComplete = callback }

    /** 设置状态变化回调。 */
    fun onStateChange(callback: (TaskState) -> Unit) = apply { this.onStateChange = callback }

    /** 设置超时回调。 */
    fun onTimeout(callback: () -> Unit) = apply { this.onTimeout = callback }

    /**
     * 以 [TaskCallback] 一次性挂载全部回调。
     *
     * 会覆盖此前单独设置的 [onSuccess]/[onError]/[onComplete]/[onStateChange]，
     * 因此应在链式调用的起始处使用。[onTimeout] 不在 [TaskCallback] 覆盖范围内，需单独设置。
     */
    fun callback(callback: TaskCallback<T>) = apply {
        this.onSuccess = callback::onSuccess
        this.onError = callback::onError
        this.onComplete = callback::onComplete
        this.onStateChange = callback::onStateChanged
    }

    /**
     * 构建 [Task]。
     *
     * @throws IllegalArgumentException 未设置 [block]
     */
    fun build(): Task<T> {
        val body = requireNotNull(block) { "Task block must not be null" }
        return Task(
            id = id,
            name = name ?: "Task-$id",
            priority = priority,
            timeout = timeout,
            block = body,
            onSuccess = onSuccess,
            onError = onError,
            onComplete = onComplete,
            onStateChange = onStateChange,
            onTimeout = onTimeout,
        )
    }
}

/**
 * DSL 风格的任务构建入口。
 *
 * ```kotlin
 * val t = task<String> {
 *     name = "LoadData"
 *     priority = TaskPriority.HIGH
 *     timeout = 30.seconds
 *     block { progress -> fetchData(progress) }
 *     onSuccess { println(it) }
 * }
 * ```
 *
 * ## 相对旧实现的调整
 *
 * 去掉了 `reified`。旧签名 `inline fun <reified T> task(...)` 中的 `T` 从未参与
 * 任何需要具体化的操作（无 `is T`、无 `T::class`、无 `typeOf<T>()`），
 * `reified` 只是把该函数强制变成 inline，且会将 [TaskBuilder] 的全部公有 API
 * 锁进调用方字节码，反而增加二进制兼容负担。
 *
 * @param T 执行结果类型
 * @param configure 配置 [TaskBuilder] 的 lambda
 */
fun <T> task(configure: TaskBuilder<T>.() -> Unit): Task<T> =
    TaskBuilder<T>().apply(configure).build()
