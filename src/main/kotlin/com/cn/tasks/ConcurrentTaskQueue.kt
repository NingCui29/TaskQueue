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
import kotlinx.coroutines.sync.Semaphore
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

/**
 * 并发任务队列。
 *
 * 以 [Semaphore] 限制同时在跑的任务数；等待中的任务按优先级降序、同优先级 FIFO 出队。
 *
 * ```kotlin
 * val queue = ConcurrentTaskQueue(scope, maxConcurrency = 4)
 * queue.enqueue(priority = TaskPriority.HIGH) { syncRemote() }
 * queue.awaitCompletion { TaskLog.d("全部完成") }
 * ```
 *
 * ## 相对旧 `com.cn.core.task.ConcurrentTaskQueue` 的调整
 *
 * | # | 旧实现问题 | 影响 | 本实现 |
 * |---|-----------|------|--------|
 * | 1 | 入队即 `executeTask` 启动协程去抢信号量 | **优先级形同虚设**——执行顺序由信号量唤醒次序决定，而非任务优先级；文档宣称的"按优先级排序"从未生效 | 改为许可驱动的拉取模型：调度循环先取得许可，再从优先级队列 `poll` 出当前最高优先级任务 |
 * | 2 | 等待集合为 `ConcurrentHashMap`，位次由 `keys` 遍历顺序给出 | 哈希序与执行序无关，位次值无意义；且 `Task.sequence` 在该队列中从未赋值，恒为 0，FIFO 无从谈起 | 换成 `PriorityQueue` + 单调递增序号 |
 * | 3 | [cancel] 先 `remove` 再 `return ... \|\| pendingTasks.containsKey(taskId)` | 判据在移除后必为 false，取消一个纯等待任务恒返回 `false`，与实际结果相反 | 依据"是否命中"直接返回 |
 * | 4 | [RejectionPolicy.BLOCK] 分支直接入队 | 该策略等同无界，突破 `maxCapacity` | 用 `Condition.await` 真正阻塞至出现空位 |
 * | 5 | [RejectionPolicy.CALLER_RUNS] 在 `queueLock` 内 `runBlocking` | 持锁执行任意长任务，阻塞其余全部入队与查询 | 决策在锁内，就地执行移至锁外 |
 * | 6 | `onRejected` 在所有溢出分支无条件触发 | `DISCARD_OLDEST` 与 `CALLER_RUNS` 也上报"被拒绝" | 仅 `THROW_EXCEPTION` / `DISCARD` 触发 |
 * | 7 | 淘汰目标按 `minByOrNull { it.priority.ordinal }` | 同优先级下未取最早入队者 | 按 (优先级, 入队序号) 升序取最小 |
 * | 8 | [awaitCompletion] 以 50 ms 间隔轮询 `isIdle` | 空转唤醒，且完成时刻最多滞后一个轮询周期 | 直接挂起等待 [state] 收敛，无轮询 |
 * | 9 | `enqueue` 中三处 `result.task as Task<T>` | 非受检强转 | 直接返回入参 |
 * | 10 | `maxConcurrency` 未校验 | 传 0 或负数时由 `Semaphore` 抛出，信息不指向调用点 | 构造期 `require` |
 *
 * @param scope 承载调度循环与任务执行的协程作用域
 * @param maxConcurrency 最大并发数，默认 [DEFAULT_CONCURRENCY]
 * @param dispatcher 任务执行调度器，默认 [Dispatchers.Default]
 * @param backpressureConfig 背压配置，默认无界
 *
 * @see SerialTaskQueue
 * @see Task
 * @see BackpressureConfig
 */
class ConcurrentTaskQueue(
    private val scope: CoroutineScope,
    private val maxConcurrency: Int = DEFAULT_CONCURRENCY,
    private val dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val backpressureConfig: BackpressureConfig = BackpressureConfig.unbounded(),
) {

    init {
        require(maxConcurrency > 0) {
            "maxConcurrency must be positive, but was $maxConcurrency"
        }
    }

    private val semaphore = Semaphore(maxConcurrency)

    private val sequenceCounter = AtomicLong(0L)

    private val queueLock = ReentrantLock()
    private val executionContext = QueueExecutionContext()
    private var stateVersion = 0L

    /** 队列由满转为不满的条件量，服务于 [RejectionPolicy.BLOCK]。 */
    private val notFull = queueLock.newCondition()

    private val taskQueue: PriorityQueue<Task<*>> = PriorityQueue(INITIAL_CAPACITY, TASK_ORDER)

    private val runningJobs = ConcurrentHashMap<String, Job>()

    /** 已持有许可、正在通知队列位置、尚未登记 Job 的任务；由 queueLock 保护。 */
    private val dispatchingTasks = mutableMapOf<String, Task<*>>()

    /** 在 queueLock 内递增；覆盖已经出队、尚未登记 Job 的任务。 */
    private var cancellationGeneration = 0L

    private val activeCount = AtomicInteger(0)

    private val isShutdown = AtomicBoolean(false)

    private val isDispatching = AtomicBoolean(false)
    private val dispatchJob = AtomicReference<Job?>(null)
    private val ownerObserver = AtomicReference<DisposableHandle?>(null)

    private val _state = QueueStateFlow<ConcurrentState>(ConcurrentState.Idle)

    init {
        ownerObserver.set(observeOwnerCancellation(scope, ::shutdown))
        if (isShutdown.get()) ownerObserver.getAndSet(null)?.dispose()
    }

    /**
     * 并发队列状态。
     *
     * ## 相对旧实现的调整
     *
     * 由 `sealed class` 改为 `sealed interface`，去掉一层空抽象基类。
     */
    sealed interface ConcurrentState {

        /** 无任务执行，且无任务等待。 */
        data object Idle : ConcurrentState

        /**
         * 运行中。
         *
         * @param activeCount 正在执行的任务数
         * @param pendingCount 等待中的任务数
         */
        data class Running(val activeCount: Int, val pendingCount: Int) : ConcurrentState

        /** 已停止接收，正在等待自身在途任务清理。 */
        data class Closing(val runningCount: Int) : ConcurrentState

        /** 已关闭且自身在途任务已完成清理。 */
        data object Shutdown : ConcurrentState

        /**
         * 容量耗尽。
         *
         * @param activeCount 正在执行的任务数
         * @param pendingCount 等待中的任务数
         * @param maxCapacity 队列容量上限
         */
        data class Full(
            val activeCount: Int,
            val pendingCount: Int,
            val maxCapacity: Int,
        ) : ConcurrentState
    }

    /** 可观察的队列状态。 */
    val state: StateFlow<ConcurrentState> = _state

    /** 正在执行的任务数。 */
    val runningTaskCount: Int
        get() = activeCount.get()

    /** 等待中的任务数。 */
    val pendingTaskCount: Int
        get() = queueLock.withLock { taskQueue.size }

    /** 容量上限，[BackpressureConfig.UNBOUNDED] 表示无界。 */
    val maxCapacity: Int
        get() = backpressureConfig.maxCapacity

    /** 容量是否已耗尽。无界队列恒为 false。 */
    val isFull: Boolean
        get() = backpressureConfig.isBackpressureEnabled && pendingTaskCount >= maxCapacity

    /** 是否空闲：无等待任务且无执行中任务。 */
    val isIdle: Boolean
        get() = queueLock.withLock { activeCount.get() == 0 && taskQueue.isEmpty() && !isDispatching.get() }

    /** 是否已不可逆地停止接收任务。 */
    val isClosed: Boolean
        get() = isShutdown.get()

    /** 已关闭并且在途任务、调度循环均已完成清理。 */
    val isTerminated: Boolean get() = isClosed && isIdle

    // region 入队

    /**
     * 入队并返回详细结果。
     *
     * 队列已关闭时直接拒绝；容量耗尽时按 [BackpressureConfig.policy] 处置。
     *
     * @param task 待执行任务
     * @return 该次入队的归宿，见 [EnqueueResult]
     * @throws IllegalArgumentException 本队列已有相同 ID 的等待中或执行中任务
     * @throws InterruptedException 策略为 [RejectionPolicy.BLOCK] 且等待空位期间线程被中断
     */
    fun <T> enqueueWithResult(task: Task<T>): EnqueueResult {
        if (isShutdown.get() || !scope.isActive) {
            return closedRejection(task, pendingTaskCount, maxCapacity, backpressureConfig.onRejected)
        }

        // 锁内只做集合操作与策略决策；外部回调、状态通知与就地执行一律移到锁外。
        var decided: EnqueueResult? = null
        var discardedTask: Task<*>? = null
        var overflowSize = NO_OVERFLOW
        var sizeAfterEnqueue = NOT_ENQUEUED
        var rejectedAfterShutdown = false

        queueLock.withLock {
            // shutdown 可能在入口检查之后，或 BLOCK 等待期间发生。
            // 必须在与队列清空相同的锁内决定能否继续接收任务。
            if (isShutdown.get() || !scope.isActive) {
                rejectedAfterShutdown = true
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
                        rejectedAfterShutdown = true
                        return@withLock
                    }
                    doEnqueue(task)
                    sizeAfterEnqueue = taskQueue.size
                    decided = EnqueueResult.Success(task)
                }
            }
        }

        if (rejectedAfterShutdown) {
            return executionContext.notify {
                closedRejection(task, pendingTaskCount, maxCapacity, backpressureConfig.onRejected)
            }
        }

        return executionContext.notify {
            val result: EnqueueResult
            try {
                if (overflowSize != NO_OVERFLOW) {
                    updateState()
                }

                discardedTask?.let { victim ->
                    executionContext.notifyAndRefresh(
                        action = {
                            victim.notifyCancellation()
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
                    updateState()
                    startDispatchLoop()
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
            result.cause == EnqueueResult.RejectionCause.CAPACITY &&
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
     * 取消指定任务。
     *
     * 等待中与执行中的任务均可取消。
     *
     * @param taskId 目标任务 id
     * @return 是否命中并取消
     */
    fun cancel(taskId: String): Boolean {
        val (pending, job) = queueLock.withLock {
            val target = taskQueue.firstOrNull { it.id == taskId }
            if (target != null) {
                taskQueue.remove(target)
                notFull.signalAll()
            }
            val notStarted = target ?: dispatchingTasks[taskId]
            // 在与 Job 登记相同的锁内置取消标志，避免交接后才取消导致任务抢先执行。
            notStarted?.cancel()
            notStarted to runningJobs[taskId]
        }
        if (pending != null || job != null) {
            try {
                pending?.let {
                    executionContext.notifyAndRefresh(
                        action = { it.notifyCancellation() },
                        refresh = ::notifyPendingPositions,
                    )
                }
            } finally {
                try {
                    job?.cancel()
                } finally {
                    updateState()
                }
            }
            return true
        }
        return false
    }

    /** 取消全部任务，包括正在执行的任务。 */
    fun cancelAll() {
        val (pending, running) = queueLock.withLock {
            cancellationGeneration++
            val snapshot = taskQueue.toList() + dispatchingTasks.values
            snapshot.forEach { it.cancel() }
            taskQueue.clear()
            notFull.signalAll()
            snapshot to runningJobs.values.toList()
        }
        // 外部状态回调可以提交新任务；只取消锁内截取的旧 Job。
        try {
            executionContext.notify { cancelOwnedTasks(running, pending) }
        } finally {
            updateState()
        }
    }

    /**
     * 关闭队列。
     *
     * 关闭后不再接受新任务，并取消全部现存任务。该操作不可逆。
     */
    fun shutdown() {
        val changed = queueLock.withLock {
            val changed = isShutdown.compareAndSet(false, true)
            notFull.signalAll()
            changed
        }
        if (!changed) return
        ownerObserver.getAndSet(null)?.dispose()
        try {
            dispatchJob.get()?.cancel()
        } finally {
            cancelAll()
        }
    }

    /**
     * 挂起等待队列排空。
     *
     * 关闭只停止接收；仍等待已运行任务的清理和调度循环结束。
     *
     * ## 相对旧实现的调整
     *
     * 旧版以 `delay(50)` 轮询 `isIdle`，既有空转唤醒，完成时刻也最多滞后一个周期；
     * 此处以内部状态版本唤醒并检查实际所有权，无轮询，也不依赖可能滞后的公开展示状态。
     */
    suspend fun awaitCompletion() {
        _state.awaitCondition { isIdle }
    }

    /**
     * 等待队列排空后回调。
     *
     * 回调在 [scope] 上执行，并受同队列同步通知的重入保护。回调内满容量 BLOCK 提交
     * 返回 CAPACITY 拒绝，便捷 enqueue 抛 [TaskQueueFullException]，避免阻塞排空任务的调度线程。
     *
     * @param onComplete 排空后的回调
     */
    fun awaitCompletion(onComplete: () -> Unit) {
        scope.launch {
            awaitCompletion()
            executionContext.notify(onComplete)
        }
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
                        task.prepareForEnqueue()
                        runningJobs[task.id] = job
                        activeCount.incrementAndGet()
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
                            executionContext.notify { task.notifyCancellation() }
                        }
                    } finally {
                        runningJobs.remove(task.id, job)
                        activeCount.decrementAndGet()
                        updateState()
                    }
                }
                updateState()
                task.execute()
            })
        } catch (error: Exception) {
            Result.failure(error)
        }
        admissionFailure?.let { throw it }
        if (rejectedClosed) return closedRejection(task, pendingTaskCount, maxCapacity, null)
        // 外层异常来自取消，内层来自任务自身失败。
        val error = outcome.exceptionOrNull() ?: outcome.getOrNull()?.exceptionOrNull()
        return EnqueueResult.CallerExecuted(task, success = error == null, error = error)
    }

    /** ID 在本队列拥有任务期间必须唯一；检查与登记共用 [queueLock]。 */
    private fun requireUniqueTaskId(taskId: String) {
        require(taskQueue.none { it.id == taskId } && taskId !in dispatchingTasks &&
            !runningJobs.containsKey(taskId)
        ) { "Task ID '$taskId' is already owned by $QUEUE_NAME" }
    }

    /** 必须在持有 [queueLock] 时调用。 */
    private fun doEnqueue(task: Task<*>) {
        task.prepareForEnqueue()
        task.sequence = sequenceCounter.getAndIncrement()
        taskQueue.offer(task)
    }

    private fun fullReason(currentSize: Int): String =
        "Queue is full (size: $currentSize, max: ${backpressureConfig.maxCapacity})"

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
     * `PriorityQueue` 仅保证堆顶有序，故须先按 [TASK_ORDER] 排序再计算位次。回调在锁外触发。
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

    private fun updateState() {
        val snapshot = queueLock.withLock {
            val active = activeCount.get()
            val pending = taskQueue.size
            val next = when {
                isShutdown.get() && (active != 0 || pending != 0 || isDispatching.get()) -> ConcurrentState.Closing(active)
                isShutdown.get() -> ConcurrentState.Shutdown
                active == 0 && pending == 0 && !isDispatching.get() -> ConcurrentState.Idle
                backpressureConfig.isBackpressureEnabled && pending >= backpressureConfig.maxCapacity ->
                    ConcurrentState.Full(active, pending, backpressureConfig.maxCapacity)
                else -> ConcurrentState.Running(active, pending)
            }
            QueueStateSnapshot(++stateVersion, next)
        }
        executionContext.notify { _state.publish(snapshot) }
    }

    /**
     * 启动调度循环。
     *
     * 调度模型：**先取得许可，再取任务**。
     * 只有拿到信号量许可的那一刻才从优先级队列 `poll`，因此出队顺序严格由
     * [TASK_ORDER] 决定；旧实现"入队即启动协程抢许可"会让执行顺序退化为唤醒顺序，
     * 优先级不起作用。
     *
     * 以 [isDispatching] 做单例守卫；循环退出后若仍有残留任务会再次自举。
     */
    private fun startDispatchLoop() {
        if (isShutdown.get() || !scope.isActive) return
        if (!isDispatching.compareAndSet(false, true)) return

        val worker = scope.launch(start = CoroutineStart.LAZY) {
            while (scope.isActive && !isShutdown.get()) {
                // acquire 的快速路径不会检查取消；旧 worker 不能再取走下一项等待任务。
                currentCoroutineContext().ensureActive()
                semaphore.acquire()
                val next = queueLock.withLock {
                    taskQueue.poll()?.let { task ->
                        dispatchingTasks[task.id] = task
                        activeCount.incrementAndGet()
                        notFull.signalAll()
                        task to cancellationGeneration
                    }
                }
                if (next == null) {
                    // 无任务可取，归还许可并结束本轮循环。
                    semaphore.release()
                    break
                }
                var notificationFailure: Throwable? = null
                try {
                    executionContext.notifyForDispatch { notifyPendingPositions() }
                } catch (failure: Throwable) {
                    if (failure !is Exception || failure is CancellationException) {
                        // 即时调度器可能在 start 内进入业务体；失败保留项须在启动前结束。
                        throw cancelDispatchReservation(next.first, failure)
                    }
                    // 普通通知异常仍交付已接收任务，随后再传播通知失败。
                    notificationFailure = failure
                }
                try {
                    // 在途计数已随 poll 登记；所有同步通知都在任务 Job 交接前结束。
                    updateState()
                } catch (failure: Throwable) {
                    val combined = notificationFailure?.let { combineTaskFailures(it, failure) } ?: failure
                    if (failure !is Exception || failure is CancellationException) {
                        throw cancelDispatchReservation(next.first, combined)
                    }
                    notificationFailure = combined
                }
                try {
                    // 即使通知正常返回或仅抛普通异常，仍须核对实际派发协程是否已取消。
                    currentCoroutineContext().ensureActive()
                } catch (cancellation: CancellationException) {
                    throw cancelDispatchReservation(
                        next.first,
                        notificationFailure?.let { combineTaskFailures(it, cancellation) } ?: cancellation,
                    )
                }
                try {
                    launchTask(next.first, next.second)
                } catch (launchFailure: Throwable) {
                    throw notificationFailure?.let { combineTaskFailures(it, launchFailure) } ?: launchFailure
                }
                notificationFailure?.let { throw it }
            }
        }
        dispatchJob.set(worker)
        worker.invokeOnCompletion {
            dispatchJob.compareAndSet(worker, null)
            isDispatching.set(false)
            updateState()
            val hasRemaining = queueLock.withLock { taskQueue.isNotEmpty() }
            if (hasRemaining && !isShutdown.get() && scope.isActive) startDispatchLoop()
        }
        if (isShutdown.get()) worker.cancel() else worker.start()
    }

    /** 取消尚未交给执行 Job 的保留任务，唯一归还许可并保留原始派发错误。 */
    private fun cancelDispatchReservation(task: Task<*>, dispatchFailure: Throwable): Throwable {
        var failure = dispatchFailure
        task.cancel()
        try {
            executionContext.notify { task.notifyCancellation() }
        } catch (notificationFailure: Throwable) {
            failure = combineTaskFailures(failure, notificationFailure)
        } finally {
            val released = queueLock.withLock {
                if (dispatchingTasks[task.id] === task) {
                    dispatchingTasks.remove(task.id)
                    activeCount.decrementAndGet()
                    true
                } else false
            }
            if (released) {
                try {
                    semaphore.release()
                } catch (releaseFailure: Throwable) {
                    failure = combineTaskFailures(failure, releaseFailure)
                }
            }
        }
        try {
            updateState()
        } catch (publicationFailure: Throwable) {
            failure = combineTaskFailures(failure, publicationFailure)
        }
        return failure
    }

    /**
     * 启动单个任务协程。
     *
     * 以 [CoroutineStart.LAZY] 创建后先登记再启动——若先启动后登记，
     * 快速结束的任务会在登记前就完成清理，[runningJobs] 将残留已完成的 `Job`。
     */
    private fun launchTask(task: Task<*>, generation: Long) {
        val job = scope.launch(dispatcher + executionContext.coroutineContext, start = CoroutineStart.LAZY) {
            val cancelledBeforeStart = queueLock.withLock { generation != cancellationGeneration }
            if (task.isCancelled || isShutdown.get() || cancelledBeforeStart) {
                task.cancel()
                executionContext.notify { task.notifyCancellation() }
                return@launch
            }
            task.execute()
        }
        val admitted = queueLock.withLock {
            // Map.remove(key, value) 需要 API 24；交接仍在队列锁内按原实例移除。
            if (dispatchingTasks[task.id] === task) dispatchingTasks.remove(task.id)
            if (generation != cancellationGeneration || isShutdown.get()) {
                false
            } else {
                runningJobs[task.id] = job
                true
            }
        }
        // 被取消的 LAZY/已派发 Job 可能从未进入 body，finally 不会执行。
        // 完成处理器负责唯一一次归还许可，也覆盖这种取消路径。
        job.invokeOnCompletion {
            try {
                if (job.isCancelled) {
                    executionContext.notify { task.notifyCancellation() }
                }
            } finally {
                activeCount.decrementAndGet()
                runningJobs.remove(task.id, job)
                semaphore.release()
                updateState()
            }
        }
        if (admitted) job.start() else job.cancel()
    }

    // endregion

    companion object {

        /** 默认最大并发数。 */
        const val DEFAULT_CONCURRENCY: Int = 4

        /** 不限并发数，实际仍受系统资源约束。 */
        const val UNLIMITED_CONCURRENCY: Int = Int.MAX_VALUE

        private const val TAG = "ConcurrentTaskQueue"

        private const val QUEUE_NAME = "ConcurrentTaskQueue"

        /** `PriorityQueue` 的默认初始容量，与 JDK 保持一致。 */
        private const val INITIAL_CAPACITY = 11

        /** 未发生溢出的哨兵值。 */
        private const val NO_OVERFLOW = -1

        /** 未入队的哨兵值。 */
        private const val NOT_ENQUEUED = -1

        /** 出队顺序：优先级高者先，同优先级按入队序号升序。 */
        private val TASK_ORDER: Comparator<Task<*>> =
            compareByDescending<Task<*>> { it.priority }.thenBy { it.sequence }

        /** 淘汰顺序：优先级最低者先，同优先级取最早入队者。 */
        private val DISCARD_ORDER: Comparator<Task<*>> =
            compareBy<Task<*>> { it.priority }.thenBy { it.sequence }
    }
}
