package com.cn.tasks

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.isActive
import java.util.concurrent.ConcurrentHashMap

/**
 * 任务管理器
 *
 * 全局单例对象，用于统一管理多个任务队列。
 * 提供创建、获取、移除队列的接口，以及便捷的任务提交方法。
 *
 * 特性：
 * - 全局单例管理
 * - 支持创建命名队列
 * - 提供默认队列
 * - 统一的任务提交接口
 * - 支持全局任务取消
 * - 支持任务状态回调
 * - 队列状态监控
 *
 * ## 作用域所有权
 *
 * 1. **作用域由宿主注入**：本库不创建自己的根作用域。宿主通过 [initialize]
 *    传入一个生命周期覆盖整个进程的 `CoroutineScope`（通常是应用级作用域），
 *    在启动期注入一次即可；未注入就提交任务会立即报错，而不是静默使用一份游离作用域。
 * 2. **[shutdown] 不取消注入的作用域**：该作用域由宿主拥有，可能被其他组件共享，
 *    任务管理器关闭时仅清空自身队列与取消在途任务，绝不触碰它。
 *
 * 使用示例：
 * ```kotlin
 * // 应用启动时注入（仅需一次）
 * TaskManager.initialize(applicationScope)
 *
 * // 提交有序任务（带状态回调）
 * TaskManager.submitSerial(
 *     priority = TaskPriority.HIGH,
 *     block = { progress -> fetchData(progress) },
 *     onSuccess = { result -> handleResult(result) },
 *     onStateChange = { state -> handleState(state) }
 * )
 *
 * // 提交并发任务
 * TaskManager.submitConcurrent(
 *     priority = TaskPriority.NORMAL,
 *     block = { progress -> downloadImage(url, progress) },
 *     onSuccess = { bitmap -> displayImage(bitmap) }
 * )
 *
 * // 创建自定义队列
 * val customQueue = TaskManager.createSerialQueue("custom")
 * customQueue.enqueue { doSomething() }
 * ```
 *
 * @see SerialTaskQueue
 * @see ConcurrentTaskQueue
 * @see Task
 * @see TaskState
 */
object TaskManager {

    private val lifecycleLock = Any()
    private var isShutdown = false

    private lateinit var _scope: CoroutineScope

    /**
     * 注入应用级协程作用域。
     *
     * 必须在任何任务提交前调用一次，传入由宿主提供、生命周期覆盖整个进程的
     * `CoroutineScope`；本库不会持有除该作用域之外的任何宿主对象。
     * 在宿主启动期完成注入即可（Android 上可借助 `androidx.startup` Initializer）。
     *
     * 显式重新初始化时关闭原队列并建立新生命周期；同一个有效 scope 的重复注入无操作。
     */
    fun initialize(scope: CoroutineScope) {
        val previous = synchronized(lifecycleLock) {
            if (!isShutdown && ::_scope.isInitialized && _scope === scope) return
            val snapshot = serialQueues.values.toList() to concurrentQueues.values.toList()
            serialQueues.clear()
            concurrentQueues.clear()
            _scope = scope
            isShutdown = false
            snapshot
        }
        closeQueues(previous.first, previous.second)
    }

    /**
     * 已注入的应用级作用域。
     *
     * 若尚未调用 [initialize] 即使用任务提交能力，将抛出明确错误以提示先行注入，
     * 而不是静默创建一份与宿主生命周期脱节的游离作用域。
     */
    private val scope: CoroutineScope
        get() {
            if (!::_scope.isInitialized) {
                error(
                    "TaskManager 尚未初始化：请在使用前调用 " +
                        "TaskManager.initialize(scope)，传入宿主的应用级 CoroutineScope。"
                )
            }
            return _scope
        }

    private val serialQueues = ConcurrentHashMap<String, SerialTaskQueue>()
    private val concurrentQueues = ConcurrentHashMap<String, ConcurrentTaskQueue>()

    private fun requireOpen() {
        if (isShutdown || !scope.isActive) throw TaskQueueClosedException("TaskManager")
    }

    private fun resolveSerialQueue(name: String): SerialTaskQueue = synchronized(lifecycleLock) {
        requireOpen()
        serialQueues[name] ?: defaultSerialQueue
    }

    private fun resolveConcurrentQueue(name: String): ConcurrentTaskQueue = synchronized(lifecycleLock) {
        requireOpen()
        concurrentQueues[name] ?: defaultConcurrentQueue
    }

    private fun closeQueues(serial: List<SerialTaskQueue>, concurrent: List<ConcurrentTaskQueue>) {
        val actions = serial.map { queue -> { queue.shutdown() } } +
            concurrent.map { queue -> { queue.shutdown() } }
        runAllQueueActions(actions)
    }

    private fun runAllQueueActions(actions: List<() -> Unit>) {
        var failure: Throwable? = null
        actions.forEach { action ->
            try {
                action()
            } catch (error: Throwable) {
                failure = failure?.let { combineTaskFailures(it, error) } ?: error
            }
        }
        failure?.let { throw it }
    }

    /**
     * 默认有序队列
     *
     * 当调用 [submitSerial] 且未指定队列名时使用此队列。
     */
    private val defaultSerialQueue: SerialTaskQueue
        get() = createSerialQueue("default_serial")

    /**
     * 默认并发队列
     *
     * 当调用 [submitConcurrent] 且未指定队列名时使用此队列。
     */
    private val defaultConcurrentQueue: ConcurrentTaskQueue
        get() = createConcurrentQueue("default_concurrent")

    /**
     * 创建有序任务队列
     *
     * 创建一个新的有序任务队列，如果同名队列已存在则返回现有队列。
     *
     * @param name 队列名称，用于后续获取队列
     * @param dispatcher 协程调度器，默认为 [Dispatchers.Default]
     * @return 创建或获取的有序任务队列
     */
    fun createSerialQueue(
        name: String,
        dispatcher: CoroutineDispatcher = Dispatchers.Default
    ): SerialTaskQueue {
        return synchronized(lifecycleLock) {
            requireOpen()
            serialQueues.getOrPut(name) { SerialTaskQueue(scope, dispatcher) }
        }
    }

    /**
     * 创建并发任务队列
     *
     * 创建一个新的并发任务队列，如果同名队列已存在则返回现有队列。
     *
     * @param name 队列名称，用于后续获取队列
     * @param maxConcurrency 最大并发数，默认为 [ConcurrentTaskQueue.DEFAULT_CONCURRENCY]
     * @param dispatcher 协程调度器，默认为 [Dispatchers.Default]
     * @return 创建或获取的并发任务队列
     */
    fun createConcurrentQueue(
        name: String,
        maxConcurrency: Int = ConcurrentTaskQueue.DEFAULT_CONCURRENCY,
        dispatcher: CoroutineDispatcher = Dispatchers.Default
    ): ConcurrentTaskQueue {
        return synchronized(lifecycleLock) {
            requireOpen()
            concurrentQueues.getOrPut(name) { ConcurrentTaskQueue(scope, maxConcurrency, dispatcher) }
        }
    }

    /**
     * 获取有序任务队列
     *
     * 根据名称获取已创建的有序任务队列。
     *
     * @param name 队列名称
     * @return 对应的有序任务队列，如果不存在则返回 null
     */
    fun getSerialQueue(name: String): SerialTaskQueue? = serialQueues[name]

    /**
     * 获取并发任务队列
     *
     * 根据名称获取已创建的并发任务队列。
     *
     * @param name 队列名称
     * @return 对应的并发任务队列，如果不存在则返回 null
     */
    fun getConcurrentQueue(name: String): ConcurrentTaskQueue? = concurrentQueues[name]

    /**
     * 移除队列
     *
     * 移除指定名称的队列（包括有序和并发队列）。
     * 移除前会取消该队列中的所有任务。
     *
     * @param name 要移除的队列名称
     */
    fun removeQueue(name: String) {
        val removed = synchronized(lifecycleLock) {
            listOfNotNull(serialQueues.remove(name)) to listOfNotNull(concurrentQueues.remove(name))
        }
        closeQueues(removed.first, removed.second)
    }

    /**
     * 提交有序任务
     *
     * 将任务提交到指定的有序队列中执行。
     * 如果队列不存在，则使用默认队列。
     *
     * @param T 任务返回值类型
     * @param task 要执行的任务
     * @param queueName 队列名称，默认为 "default_serial"
     * @return 提交的任务实例
     */
    fun <T> submitSerial(
        task: Task<T>,
        queueName: String = "default_serial"
    ): Task<T> {
        val queue = try { resolveSerialQueue(queueName) } catch (closed: TaskQueueClosedException) {
            task.cancel()
            task.updateState(TaskState.Cancelled)
            throw closed
        }
        return queue.enqueue(task)
    }

    /**
     * 创建并提交有序任务（带进度报告）
     *
     * 便捷方法，创建一个新任务并提交到有序队列。
     *
     * @param T 任务返回值类型
     * @param queueName 队列名称，默认为 "default_serial"
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数，接收进度报告器
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并提交的任务实例
     */
    fun <T> submitSerial(
        queueName: String = "default_serial",
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        val queue = resolveSerialQueue(queueName)
        return queue.enqueue(priority, block, onSuccess, onError, onStateChange)
    }

    /**
     * 创建并提交有序任务（简化版本，无进度报告）
     *
     * @param T 任务返回值类型
     * @param queueName 队列名称，默认为 "default_serial"
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并提交的任务实例
     */
    fun <T> submitSerial(
        queueName: String = "default_serial",
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend () -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        val queue = resolveSerialQueue(queueName)
        return queue.enqueue(priority, block, onSuccess, onError, onStateChange)
    }

    /**
     * 提交并发任务
     *
     * 将任务提交到指定的并发队列中执行。
     * 如果队列不存在，则使用默认队列。
     *
     * @param T 任务返回值类型
     * @param task 要执行的任务
     * @param queueName 队列名称，默认为 "default_concurrent"
     * @return 提交的任务实例
     */
    fun <T> submitConcurrent(
        task: Task<T>,
        queueName: String = "default_concurrent"
    ): Task<T> {
        val queue = try { resolveConcurrentQueue(queueName) } catch (closed: TaskQueueClosedException) {
            task.cancel()
            task.updateState(TaskState.Cancelled)
            throw closed
        }
        return queue.enqueue(task)
    }

    /**
     * 创建并提交并发任务（带进度报告）
     *
     * 便捷方法，创建一个新任务并提交到并发队列。
     *
     * @param T 任务返回值类型
     * @param queueName 队列名称，默认为 "default_concurrent"
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数，接收进度报告器
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并提交的任务实例
     */
    fun <T> submitConcurrent(
        queueName: String = "default_concurrent",
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        val queue = resolveConcurrentQueue(queueName)
        return queue.enqueue(priority, block, onSuccess, onError, onStateChange)
    }

    /**
     * 创建并提交并发任务（简化版本，无进度报告）
     *
     * @param T 任务返回值类型
     * @param queueName 队列名称，默认为 "default_concurrent"
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并提交的任务实例
     */
    fun <T> submitConcurrent(
        queueName: String = "default_concurrent",
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend () -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        val queue = resolveConcurrentQueue(queueName)
        return queue.enqueue(priority, block, onSuccess, onError, onStateChange)
    }

    /**
     * 取消指定任务
     *
     * 在所有队列中查找并取消指定ID的任务。
     *
     * @param taskId 要取消的任务ID
     * @return 如果成功取消任务则返回 true，否则返回 false
     */
    fun cancelTask(taskId: String): Boolean {
        var cancelled = false
        val actions: List<() -> Unit> = synchronized(lifecycleLock) {
            serialQueues.values.map { queue -> { if (queue.cancel(taskId)) cancelled = true } } +
                concurrentQueues.values.map { queue -> { if (queue.cancel(taskId)) cancelled = true } }
        }
        runAllQueueActions(actions)
        return cancelled
    }

    /**
     * 取消所有任务
     *
     * 取消所有队列中的所有任务。
     */
    fun cancelAll() {
        val actions = synchronized(lifecycleLock) {
            serialQueues.values.map { queue -> { queue.cancelAll() } } +
                concurrentQueues.values.map { queue -> { queue.cancelAll() } }
        }
        runAllQueueActions(actions)
    }

    /**
     * 关闭任务管理器
     *
     * 取消所有任务，清空所有队列。**不会**取消通过 [initialize] 注入的共享作用域，
     * 以避免误伤应用级协程。关闭后不应再使用此管理器。
     */
    fun shutdown() {
        val owned = synchronized(lifecycleLock) {
            if (isShutdown) return
            isShutdown = true
            val snapshot = serialQueues.values.toList() to concurrentQueues.values.toList()
            serialQueues.clear()
            concurrentQueues.clear()
            snapshot
        }
        closeQueues(owned.first, owned.second)
    }

    /**
     * 获取队列统计信息
     *
     * 获取所有队列的运行状态统计。
     *
     * @return 队列名称到统计信息的映射
     */
    fun getQueueStats(): Map<String, QueueStats> {
        val stats = mutableMapOf<String, QueueStats>()
        serialQueues.forEach { (name, queue) ->
            stats["serial_$name"] = QueueStats(
                name = name,
                type = QueueType.SERIAL,
                pendingCount = queue.pendingCount,
                runningCount = queue.runningTaskCount
            )
        }
        concurrentQueues.forEach { (name, queue) ->
            stats["concurrent_$name"] = QueueStats(
                name = name,
                type = QueueType.CONCURRENT,
                pendingCount = queue.pendingTaskCount,
                runningCount = queue.runningTaskCount
            )
        }
        return stats
    }

    /**
     * 队列统计信息
     *
     * @param name 队列名称
     * @param type 队列类型
     * @param pendingCount 待执行任务数量
     * @param runningCount 正在执行任务数量
     */
    data class QueueStats(
        val name: String,
        val type: QueueType,
        val pendingCount: Int,
        val runningCount: Int
    )

    /**
     * 队列类型
     */
    enum class QueueType {
        /**
         * 有序队列
         */
        SERIAL,

        /**
         * 并发队列
         */
        CONCURRENT
    }
}
