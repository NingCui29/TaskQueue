package com.cn.tasks

import com.cn.tasks.log.TaskLog
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

/**
 * 串行任务队列。
 *
 * 任务按优先级降序出队，同优先级按入队顺序（FIFO）执行，同一时刻至多一个任务在跑。
 *
 * ```kotlin
 * val queue = SerialTaskQueue(scope)
 * queue.enqueue(priority = TaskPriority.HIGH) { loadData() }
 *
 * // 带背压
 * val bounded = SerialTaskQueue(
 *     scope = scope,
 *     backpressureConfig = BackpressureConfig.discardOldest(maxCapacity = 100),
 * )
 * when (val r = bounded.enqueueWithResult(task)) {
 *     is EnqueueResult.Rejected -> TaskLog.w("被拒绝: ${r.reason}")
 *     else -> Unit
 * }
 * ```
 *
 * ## 相对旧 `com.cn.core.task.SerialTaskQueue` 的调整
 *
 * | # | 旧实现问题 | 影响 | 本实现 |
 * |---|-----------|------|--------|
 * | 1 | [pause] 只取消当前任务，工作循环仍继续 `poll` | 暂停后队列照常执行后续任务，暂停语义完全失效 | 引入 [paused] 标志，工作循环据此退出，[resume] 重启 |
 * | 2 | [RejectionPolicy.BLOCK] 分支直接入队 | 该策略等同无界，突破 `maxCapacity` | 用 `Condition.await` 真正阻塞至出现空位 |
 * | 3 | [RejectionPolicy.CALLER_RUNS] 在 `queueLock` 内 `runBlocking` 执行整个任务 | 持锁跑任意长任务，期间全部入队、查询、取消操作被阻塞 | 决策在锁内，就地执行移至锁外 |
 * | 4 | `onRejected` 在所有溢出分支无条件触发 | `DISCARD_OLDEST`（实际入队成功）与 `CALLER_RUNS`（已执行）也上报"被拒绝" | 仅 `THROW_EXCEPTION` / `DISCARD` 触发 |
 * | 5 | 用 `PriorityQueue.iterator()` 计算队列位次 | 该迭代器不保证有序，位次值基本错误 | 取副本按 [TASK_ORDER] 排序后计算 |
 * | 6 | 淘汰目标按 `minByOrNull { it.priority.ordinal }` 选取 | 同优先级下未选最早入队者，"丢弃最旧"名不副实 | 按 (优先级, 入队序号) 升序取最小 |
 * | 7 | `QueueState.Full` 从未被赋值 | 该状态是死代码，观察方永远收不到 | 溢出时置 `Full`，且不覆盖 `Paused` |
 * | 8 | `mutex` 与 `queueLock` 双重加锁保护同一个 `poll` | 冗余锁，徒增开销与死锁面 | 删除 `mutex` |
 * | 9 | 状态回调（`updateState`）在持有 `queueLock` 时触发 | 外部回调决定锁的持有时长，回调内再入队会加剧竞争 | 集合操作在锁内，全部回调移至锁外 |
 * | 10 | `android.util.Log.d("SerialTaskQueue", "enqueue: $result")` | 绕过统一日志出口，且无条件构造字符串 | 改用 [TaskLog] 惰性重载 |
 * | 11 | `enqueue` 中 `result.task as Task<T>` | 三处非受检强转 | 直接返回入参，强转消失 |
 *
 * @param scope 承载工作循环的协程作用域
 * @param dispatcher 任务执行调度器，默认 [Dispatchers.Default]
 * @param backpressureConfig 背压配置，默认无界
 *
 * @see ConcurrentTaskQueue
 * @see Task
 * @see BackpressureConfig
 */
class SerialTaskQueue(
    private val scope: CoroutineScope,
    private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val backpressureConfig: BackpressureConfig = BackpressureConfig.unbounded(),
) {

    private val sequenceCounter = AtomicLong(0L)

    private val queueLock = ReentrantLock()
    private val executionContext = QueueExecutionContext()
    private var stateVersion = 0L

    /** 队列由满转为不满的条件量，服务于 [RejectionPolicy.BLOCK]。 */
    private val notFull = queueLock.newCondition()

    private val taskQueue = PriorityQueue(INITIAL_CAPACITY, TASK_ORDER)

    private val isRunning = AtomicBoolean(false)

    private val paused = AtomicBoolean(false)

    private val currentJob = AtomicReference<Job?>(null)
    private val workerJob = AtomicReference<Job?>(null)
    private val isShutdown = AtomicBoolean(false)
    private val ownerObserver = AtomicReference<DisposableHandle?>(null)
    private val callerJobs = mutableMapOf<Task<*>, Job>()

    /** 从 poll 到任务 Job 完成期间的所有权记录，由 queueLock 保护。 */
    private var currentTask: Task<*>? = null

    private val _state = QueueStateFlow<QueueState>(QueueState.Idle)

    init {
        ownerObserver.set(observeOwnerCancellation(scope, ::shutdown))
        if (isShutdown.get()) ownerObserver.getAndSet(null)?.dispose()
    }

    /**
     * 队列状态。
     *
     * ## 相对旧实现的调整
     *
     * 由 `sealed class` 改为 `sealed interface`，去掉一层空抽象基类。
     */
    sealed interface QueueState {

        /** 无任务执行，且无任务等待。 */
        data object Idle : QueueState

        /** 已接收任务，正在等待调度，尚无在途执行任务。 */
        data class Waiting(val pendingCount: Int) : QueueState

        /** 已停止接收，正在等待自身在途任务清理。 */
        data class Closing(val runningCount: Int) : QueueState

        /** 已关闭且自身在途任务已完成清理。 */
        data object Shutdown : QueueState

        /**
         * 正在执行任务。
         *
         * @param taskName 当前任务名
         */
        data class Running(val taskName: String) : QueueState

        /**
         * 已暂停。
         *
         * @param pendingCount 等待中的任务数
         */
        data class Paused(val pendingCount: Int) : QueueState

        /**
         * 容量耗尽。
         *
         * 该状态在入队溢出的瞬间置位，随后会被当前实际的 [Waiting] / [Running] / [Idle] 覆盖。
         *
         * @param pendingCount 等待中的任务数
         * @param maxCapacity 队列容量上限
         */
        data class Full(val pendingCount: Int, val maxCapacity: Int) : QueueState
    }

    /** 可观察的队列状态。 */
    val state: StateFlow<QueueState> = _state

    /** 等待中的任务数，不含正在执行的任务。 */
    val pendingCount: Int
        get() = queueLock.withLock { taskQueue.size }

    /** 在途任务数，包含就地执行与取消后尚未退出的清理。 */
    val runningTaskCount: Int
        get() = queueLock.withLock { (if (currentTask != null) 1 else 0) + callerJobs.size }

    /** 容量上限，[BackpressureConfig.UNBOUNDED] 表示无界。 */
    val maxCapacity: Int
        get() = backpressureConfig.maxCapacity

    /** 容量是否已耗尽。无界队列恒为 false。 */
    val isFull: Boolean
        get() = backpressureConfig.isBackpressureEnabled && pendingCount >= maxCapacity

    /** 是否空闲：无等待任务且无执行中任务。 */
    val isIdle: Boolean
        get() = queueLock.withLock {
            taskQueue.isEmpty() && currentTask == null && callerJobs.isEmpty() && !isRunning.get()
        }

    /** 是否已不可逆地停止接收任务。 */
    val isClosed: Boolean get() = isShutdown.get()

    /** 已关闭并且在途任务、调度循环均已完成清理。 */
    val isTerminated: Boolean get() = isClosed && isIdle

    /** 是否处于暂停态。 */
    val isPaused: Boolean
        get() = paused.get()

    // region 入队

    /**
     * 入队并返回详细结果。
     *
     * 队列未满时直接入队；已满时按 [BackpressureConfig.policy] 处置。
     *
     * @param task 待执行任务
     * @return 该次入队的归宿，见 [EnqueueResult]
     * @throws IllegalArgumentException 本队列已有相同 ID 的等待中或执行中任务
     * @throws InterruptedException 策略为 [RejectionPolicy.BLOCK] 且等待空位期间线程被中断
     */
    fun <T> enqueueWithResult(task: Task<T>): EnqueueResult {
        // 锁内只做集合操作与策略决策；所有外部回调、状态通知与就地执行一律移到锁外，
        // 避免外部代码决定锁的持有时长。
        var decided: EnqueueResult? = null
        var discardedTask: Task<*>? = null
        var overflowSize = NO_OVERFLOW
        var sizeAfterEnqueue = NOT_ENQUEUED
        var rejectedClosed = false

        queueLock.withLock {
            if (isShutdown.get() || !scope.isActive) {
                rejectedClosed = true
                return@withLock
            }
            requireUniqueTaskId(task.id)
            val currentSize = taskQueue.size
            val overflow = backpressureConfig.isBackpressureEnabled &&
                currentSize >= backpressureConfig.maxCapacity

            if (!overflow) {
                doEnqueue(task)
                sizeAfterEnqueue = taskQueue.size
                decided = EnqueueResult.Success(task)
                return@withLock
            }

            overflowSize = currentSize
            val reason = fullReason(currentSize)
            when (backpressureConfig.policy) {
                RejectionPolicy.THROW_EXCEPTION -> {
                    decided = EnqueueResult.Rejected(
                        task = task,
                        reason = reason,
                        queueSize = currentSize,
                        maxCapacity = backpressureConfig.maxCapacity,
                    )
                }

                RejectionPolicy.DISCARD -> {
                    task.cancel()
                    decided = EnqueueResult.Rejected(
                        task = task,
                        reason = reason,
                        queueSize = currentSize,
                        maxCapacity = backpressureConfig.maxCapacity,
                    )
                }

                RejectionPolicy.DISCARD_OLDEST -> {
                    val victim = taskQueue.minWithOrNull(DISCARD_ORDER)
                    if (victim != null) {
                        taskQueue.remove(victim)
                        victim.cancel()
                        discardedTask = victim
                    }
                    doEnqueue(task)
                    sizeAfterEnqueue = taskQueue.size
                    decided = if (victim != null) {
                        EnqueueResult.Discarded(task, victim)
                    } else {
                        EnqueueResult.Success(task)
                    }
                }

                // 就地执行会阻塞调用者线程，必须在锁外进行，此处仅作标记。
                RejectionPolicy.CALLER_RUNS -> Unit

                RejectionPolicy.BLOCK -> {
                    if (executionContext.isEntered) {
                        decided = EnqueueResult.Rejected(
                            task, "$reason; cannot block while reentering the same queue", currentSize, maxCapacity,
                        )
                        return@withLock
                    }
                    while (!isShutdown.get() && scope.isActive && taskQueue.size >= backpressureConfig.maxCapacity) {
                        notFull.await()
                        if (!isShutdown.get() && scope.isActive) requireUniqueTaskId(task.id)
                    }
                    if (isShutdown.get() || !scope.isActive) {
                        rejectedClosed = true
                        return@withLock
                    }
                    doEnqueue(task)
                    sizeAfterEnqueue = taskQueue.size
                    decided = EnqueueResult.Success(task)
                }
            }
        }

        if (rejectedClosed) return executionContext.notify {
            closedRejection(task, pendingCount, maxCapacity, backpressureConfig.onRejected)
        }

        return executionContext.notify {
            val result: EnqueueResult
            try {
                if (sizeAfterEnqueue != NOT_ENQUEUED) {
                    publishExecutionState()
                }
                if (overflowSize != NO_OVERFLOW) {
                    markFull()
                }

                discardedTask?.let { victim ->
                    executionContext.notifyAndRefresh(
                        action = {
                            victim.updateState(TaskState.Cancelled)
                            backpressureConfig.onDiscarded?.invoke(victim, task)
                        },
                        refresh = ::notifyPendingPositions,
                    )
                }

                result = decided ?: runInCaller(task)

                if (result is EnqueueResult.Rejected) {
                    if (result.cause == EnqueueResult.RejectionCause.CAPACITY && backpressureConfig.policy == RejectionPolicy.DISCARD) {
                        task.updateState(TaskState.Cancelled)
                    }
                    backpressureConfig.onRejected?.invoke(task, result.reason)
                }

                if (sizeAfterEnqueue != NOT_ENQUEUED) {
                    if (discardedTask == null) notifyPendingPositions()
                    checkThreshold(sizeAfterEnqueue)
                }

            } finally {
                // 任务已经入队；外部通知失败不能使已接收任务失去调度。
                if (sizeAfterEnqueue != NOT_ENQUEUED) {
                    publishExecutionState()
                    processNext()
                }
            }
            TaskLog.d(TAG) { "enqueue: $result" }
            result
        }
    }

    /**
     * 入队。
     *
     * @param task 待执行任务
     * @return 入参任务本身，便于链式持有
     * @throws TaskQueueFullException 队列已满且策略为 THROW_EXCEPTION，或 BLOCK 同队列执行/通知重入
     * @throws TaskQueueClosedException 队列已关闭或所属协程作用域已结束
     * @throws InterruptedException 策略为 [RejectionPolicy.BLOCK] 且等待空位期间线程被中断
     */
    fun <T> enqueue(task: Task<T>): Task<T> {
        val result = enqueueWithResult(task)
        if (result is EnqueueResult.Rejected && result.cause == EnqueueResult.RejectionCause.CLOSED) {
            throw TaskQueueClosedException(QUEUE_NAME)
        }
        if (result is EnqueueResult.Rejected &&
            (backpressureConfig.policy == RejectionPolicy.THROW_EXCEPTION || backpressureConfig.policy == RejectionPolicy.BLOCK)
        ) {
            throw TaskQueueFullException(
                queueName = QUEUE_NAME,
                queueSize = result.queueSize,
                maxCapacity = result.maxCapacity,
            )
        }
        return task
    }

    /**
     * 就地创建任务并入队，返回详细结果。
     *
     * @param priority 优先级
     * @param block 任务体，入参为进度回报器
     * @param onSuccess 成功回调
     * @param onError 失败回调
     * @param onStateChange 状态变化回调
     */
    fun <T> enqueueWithResult(
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null,
    ): EnqueueResult = enqueueWithResult(
        Task(
            priority = priority,
            block = block,
            onSuccess = onSuccess,
            onError = onError,
            onStateChange = onStateChange,
        ),
    )

    /**
     * 就地创建任务并入队。
     *
     * @param priority 优先级
     * @param block 任务体，入参为进度回报器
     * @param onSuccess 成功回调
     * @param onError 失败回调
     * @param onStateChange 状态变化回调
     */
    fun <T> enqueue(
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null,
    ): Task<T> = enqueue(
        Task(
            priority = priority,
            block = block,
            onSuccess = onSuccess,
            onError = onError,
            onStateChange = onStateChange,
        ),
    )

    /**
     * 就地创建任务并入队，任务体无需回报进度。
     *
     * @param priority 优先级
     * @param block 任务体
     * @param onSuccess 成功回调
     * @param onError 失败回调
     * @param onStateChange 状态变化回调
     */
    fun <T> enqueue(
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend () -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null,
    ): Task<T> = enqueue(
        priority = priority,
        block = { _: ProgressReporter -> block() },
        onSuccess = onSuccess,
        onError = onError,
        onStateChange = onStateChange,
    )

    /**
     * 就地创建带超时的任务并入队。
     *
     * @param timeout 超时时长
     * @param priority 优先级
     * @param block 任务体，入参为进度回报器
     * @param onSuccess 成功回调
     * @param onError 失败回调
     * @param onTimeout 超时回调
     * @param onStateChange 状态变化回调
     */
    fun <T> enqueue(
        timeout: Duration,
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onTimeout: (() -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null,
    ): Task<T> = enqueue(
        Task(
            priority = priority,
            timeout = timeout,
            block = block,
            onSuccess = onSuccess,
            onError = onError,
            onComplete = null,
            onStateChange = onStateChange,
            onTimeout = onTimeout,
        ),
    )

    /**
     * 就地创建带超时的任务并入队，任务体无需回报进度。
     *
     * @param timeout 超时时长
     * @param priority 优先级
     * @param block 任务体
     * @param onSuccess 成功回调
     * @param onError 失败回调
     * @param onTimeout 超时回调
     * @param onStateChange 状态变化回调
     */
    fun <T> enqueue(
        timeout: Duration,
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend () -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onTimeout: (() -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null,
    ): Task<T> = enqueue(
        timeout = timeout,
        priority = priority,
        block = { _: ProgressReporter -> block() },
        onSuccess = onSuccess,
        onError = onError,
        onTimeout = onTimeout,
        onStateChange = onStateChange,
    )

    // endregion

    // region 调度控制

    /**
     * 暂停队列。
     *
     * 取消当前正在执行的任务，并阻止工作循环取出后续任务。
     * 被取消的任务不会自动重新入队；等待中的任务原样保留，[resume] 后继续。
     */
    fun pause() {
        if (isShutdown.get()) return
        val job = queueLock.withLock {
            paused.set(true)
            currentTask?.cancel()
            currentJob.get()
        }
        job?.cancel()
        publishExecutionState()
    }

    /** 从暂停态恢复，并重启工作循环。 */
    fun resume() {
        if (isShutdown.get()) return
        if (!paused.compareAndSet(true, false)) return
        try {
            publishExecutionState()
        } finally {
            processNext()
        }
    }

    /**
     * 取消指定任务。
     *
     * 作用于等待中及已出队、尚未交给执行协程的任务；已交接执行的任务不受影响。
     *
     * @param taskId 目标任务 id
     * @return 是否命中并取消
     */
    fun cancel(taskId: String): Boolean {
        val removed = queueLock.withLock {
            val queued = taskQueue.firstOrNull { it.id == taskId }
            if (queued != null) {
                taskQueue.remove(queued)
                notFull.signalAll()
            }
            val target = queued ?: currentTask?.takeIf {
                it.id == taskId && currentJob.get() == null
            }
            target?.also { it.cancel() }
        } ?: return false

        try {
            executionContext.notifyAndRefresh(
                action = { removed.updateState(TaskState.Cancelled) },
                refresh = ::notifyPendingPositions,
            )
        } finally {
            publishExecutionState()
        }
        return true
    }

    /** 取消全部现存任务；在途任务完成清理后收敛状态，不取消注入的共享 scope。 */
    fun cancelAll() {
        val (pending, jobs) = queueLock.withLock {
            val snapshot = taskQueue.toList() + listOfNotNull(currentTask)
            snapshot.forEach { it.cancel() }
            taskQueue.clear()
            notFull.signalAll()
            snapshot to (listOfNotNull(currentJob.get()) + callerJobs.values)
        }
        // 只取消锁内截取的旧 Job，回调中新提交的任务仍归后续工作循环处理。
        try {
            executionContext.notify { cancelOwnedTasks(jobs, pending) }
        } finally {
            publishExecutionState()
        }
    }

    /** 不可逆关闭并唤醒 BLOCK 生产者，只取消队列拥有的 Job。 */
    fun shutdown() {
        val changed = queueLock.withLock {
            val changed = isShutdown.compareAndSet(false, true)
            notFull.signalAll()
            changed
        }
        if (!changed) return
        ownerObserver.getAndSet(null)?.dispose()
        try {
            workerJob.get()?.cancel()
        } finally {
            cancelAll()
        }
    }

    /**
     * 清空等待队列。
     *
     * 与 [cancelAll] 的差别：不触碰正在执行的任务，且被移除的任务**不会**收到
     * [TaskState.Cancelled]——它们会永远停留在 [TaskState.Pending]。
     * 若需要状态收敛，请改用 [cancelAll]。
     */
    fun clear() {
        queueLock.withLock {
            taskQueue.clear()
            notFull.signalAll()
        }
        publishExecutionState()
    }

    // endregion

    // region 内部实现

    /** 在调用者线程就地执行任务。会阻塞当前线程直至任务结束，禁止在主线程触发。 */
    private fun <T> runInCaller(task: Task<T>): EnqueueResult {
        var rejectedClosed = false
        var admissionFailure: IllegalArgumentException? = null
        // 仅物化普通异常和取消；致命错误由 runBlocking 完成所有权清理后原样传播。
        val outcome: Result<Result<T>?> = try {
            Result.success(runBlocking(executionContext.coroutineContext) {
                val job = currentCoroutineContext()[Job]!!
                val admitted = queueLock.withLock {
                    if (isShutdown.get() || !scope.isActive) false else {
                        try {
                            requireUniqueTaskId(task.id)
                        } catch (duplicate: IllegalArgumentException) {
                            admissionFailure = duplicate
                            throw duplicate
                        }
                        callerJobs[task] = job
                        true
                    }
                }
                if (!admitted) {
                    rejectedClosed = true
                    return@runBlocking null
                }
                job.invokeOnCompletion {
                    try {
                        if (job.isCancelled) {
                            task.cancel()
                            if (task.currentState !is TaskState.Cancelled) executionContext.notify {
                                task.updateState(TaskState.Cancelled)
                            }
                        }
                    } finally {
                        queueLock.withLock {
                            // Map.remove(key, value) 需要 API 24；锁内身份检查保留原条件移除语义。
                            if (callerJobs[task] === job) callerJobs.remove(task)
                        }
                        publishExecutionState()
                    }
                }
                publishExecutionState()
                task.execute()
            })
        } catch (error: Exception) {
            Result.failure(error)
        }
        admissionFailure?.let { throw it }
        if (rejectedClosed) return closedRejection(task, pendingCount, maxCapacity, null)
        // 外层异常来自取消，内层来自任务自身失败。
        val error = outcome.exceptionOrNull() ?: outcome.getOrNull()?.exceptionOrNull()
        return EnqueueResult.CallerExecuted(task, success = error == null, error = error)
    }

    /** ID 在本队列拥有任务期间必须唯一；检查与登记共用 [queueLock]。 */
    private fun requireUniqueTaskId(taskId: String) {
        require(taskQueue.none { it.id == taskId } && currentTask?.id != taskId &&
            callerJobs.keys.none { it.id == taskId }
        ) { "Task ID '$taskId' is already owned by $QUEUE_NAME" }
    }

    /** 必须在持有 [queueLock] 时调用。 */
    private fun doEnqueue(task: Task<*>) {
        task.sequence = sequenceCounter.getAndIncrement()
        taskQueue.offer(task)
    }

    private fun fullReason(currentSize: Int): String =
        "Queue is full (size: $currentSize, max: ${backpressureConfig.maxCapacity})"

    /** 置 [QueueState.Full]，但不覆盖 [QueueState.Paused]。 */
    private fun markFull() {
        val snapshot = queueLock.withLock {
            if (isShutdown.get() || paused.get() || taskQueue.size < maxCapacity) return
            QueueStateSnapshot<QueueState>(++stateVersion, QueueState.Full(taskQueue.size, maxCapacity))
        }
        executionContext.notify { _state.publish(snapshot) }
    }

    private fun checkThreshold(currentSize: Int) {
        if (!backpressureConfig.isThresholdReached(currentSize)) return
        backpressureConfig.onThresholdReached?.invoke(
            currentSize,
            backpressureConfig.maxCapacity,
            backpressureConfig.calculateUsagePercent(currentSize),
        )
    }

    /**
     * 刷新全部等待任务的队列位次。
     *
     * `PriorityQueue` 仅保证堆顶有序，其迭代顺序与出队顺序无关，
     * 故必须先按 [TASK_ORDER] 排序再计算位次。回调在锁外触发。
     */
    private fun notifyPendingPositions() {
        val updates = queueLock.withLock {
            val ordered = taskQueue.sortedWith(TASK_ORDER)
            ordered.mapIndexed { index, task ->
                task.preparePendingState(TaskState.Pending(queuePosition = index + 1, queueSize = ordered.size))
            }
        }
        executionContext.notifyAll(updates)
    }

    private fun publishExecutionState() {
        val snapshot = queueLock.withLock {
            val next = when {
                isShutdown.get() && (currentTask != null || callerJobs.isNotEmpty() || taskQueue.isNotEmpty() || isRunning.get()) ->
                    QueueState.Closing((if (currentTask != null) 1 else 0) + callerJobs.size)
                isShutdown.get() -> QueueState.Shutdown
                paused.get() -> QueueState.Paused(taskQueue.size)
                currentTask != null -> QueueState.Running(currentTask!!.name)
                callerJobs.isNotEmpty() -> QueueState.Running(callerJobs.keys.first().name)
                taskQueue.isNotEmpty() -> QueueState.Waiting(taskQueue.size)
                else -> QueueState.Idle
            }
            QueueStateSnapshot(++stateVersion, next)
        }
        executionContext.notify { _state.publish(snapshot) }
    }

    /**
     * 启动工作循环。
     *
     * 在锁内同时核对工作循环与当前任务的所有权，旧任务退出清理前不启动替代循环。
     * 工作循环和任务各自在完成时尝试补启，避免两者完成顺序不同造成等待任务滞留。
     */
    private fun processNext() {
        val shouldStart = queueLock.withLock {
            if (paused.get() || isShutdown.get() || !scope.isActive || currentTask != null || taskQueue.isEmpty()) {
                false
            } else {
                isRunning.compareAndSet(false, true)
            }
        }
        if (!shouldStart) return

        val worker = scope.launch(start = CoroutineStart.LAZY) {
            while (scope.isActive && !paused.get() && !isShutdown.get()) {
                val task = queueLock.withLock {
                    if (paused.get() || isShutdown.get()) null else taskQueue.poll()?.also {
                        currentTask = it
                        notFull.signalAll()
                    }
                } ?: break
                var notificationFailure: Throwable? = null
                try {
                    executionContext.notifyForDispatch { notifyPendingPositions() }
                    publishExecutionState()
                } catch (failure: Throwable) {
                    if (failure !is Exception || failure is CancellationException) {
                        // 致命错误或真实调度取消不能等候尚未开始的业务体。
                        throw cancelDispatchReservation(task, failure)
                    }
                    // 普通通知异常仍保留已接收任务的调度，业务退出后再传播通知失败。
                    notificationFailure = failure
                }
                try {
                    // 同步通知可以正常返回但已取消当前 worker；新任务 Job 不能绕过这次取消。
                    currentCoroutineContext().ensureActive()
                } catch (cancellation: CancellationException) {
                    throw cancelDispatchReservation(
                        task,
                        notificationFailure?.let { combineTaskFailures(it, cancellation) } ?: cancellation,
                    )
                }
                try {
                    executeTask(task)
                } catch (executionFailure: Throwable) {
                    throw notificationFailure?.let { combineTaskFailures(it, executionFailure) } ?: executionFailure
                }
                notificationFailure?.let { throw it }
            }
        }
        workerJob.set(worker)
        // scope 在 body 启动前已取消时也必须释放调度标志，不能仅依赖 body 的 finally。
        worker.invokeOnCompletion {
            queueLock.withLock {
                if (workerJob.compareAndSet(worker, null)) isRunning.set(false)
            }
            try {
                publishExecutionState()
            } finally {
                processNext()
            }
        }
        if (isShutdown.get()) worker.cancel() else worker.start()
    }

    /** 取消尚未进入执行协程的保留任务，释放所有权并保留原始派发错误。 */
    private fun cancelDispatchReservation(task: Task<*>, dispatchFailure: Throwable): Throwable {
        var failure = dispatchFailure
        task.cancel()
        try {
            executionContext.notify { task.updateState(TaskState.Cancelled) }
        } catch (notificationFailure: Throwable) {
            failure = combineTaskFailures(failure, notificationFailure)
        } finally {
            queueLock.withLock {
                if (currentTask === task && currentJob.get() == null) currentTask = null
            }
        }
        try {
            publishExecutionState()
        } catch (publicationFailure: Throwable) {
            failure = combineTaskFailures(failure, publicationFailure)
        }
        return failure
    }

    /**
     * 执行单个任务并等待其结束。
     *
     * 任务运行在独立子 `Job` 中，便于 [pause] / [cancelAll] 精确取消而不牵连工作循环。
     */
    private suspend fun executeTask(task: Task<*>) {
        val job = scope.launch(dispatcher + executionContext.coroutineContext, start = CoroutineStart.LAZY) {
            task.execute()
        }
        val cancelledBeforeStart = queueLock.withLock {
            currentJob.set(job)
            task.isCancelled || paused.get() || isShutdown.get()
        }
        job.invokeOnCompletion {
            try {
                if (job.isCancelled) {
                    task.cancel()
                    if (task.currentState !is TaskState.Cancelled) {
                        executionContext.notify { task.updateState(TaskState.Cancelled) }
                    }
                }
            } finally {
                queueLock.withLock {
                    currentJob.compareAndSet(job, null)
                    if (currentTask === task) currentTask = null
                }
                try {
                    publishExecutionState()
                } finally {
                    // 工作循环可能已被独立取消；实际任务清理完成后才能恢复等待任务。
                    processNext()
                }
            }
        }
        if (cancelledBeforeStart) job.cancel() else job.start()
        job.join()
    }

    // endregion

    private companion object {

        const val TAG = "SerialTaskQueue"

        const val QUEUE_NAME = "SerialTaskQueue"

        /** `PriorityQueue` 的默认初始容量，与 JDK 保持一致。 */
        const val INITIAL_CAPACITY = 11

        /** 未发生溢出的哨兵值。 */
        const val NO_OVERFLOW = -1

        /** 未入队的哨兵值。 */
        const val NOT_ENQUEUED = -1

        /**
         * 出队顺序：优先级高者先，同优先级按入队序号升序。
         *
         * 直接比较 [TaskPriority] 而非其 `ordinal`——枚举本身实现了 `Comparable`，
         * 语义由声明顺序给出，无需暴露序数细节。
         */
        val TASK_ORDER: Comparator<Task<*>> =
            compareByDescending<Task<*>> { it.priority }.thenBy { it.sequence }

        /** 淘汰顺序：优先级最低者先，同优先级取最早入队者。 */
        val DISCARD_ORDER: Comparator<Task<*>> =
            compareBy<Task<*>> { it.priority }.thenBy { it.sequence }
    }
}
