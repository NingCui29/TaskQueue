package com.cn.tasks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.StateFlow

/**
 * 任务队列作用域
 *
 * 提供独立的生命周期管理的任务队列封装。
 * 根据配置的并发数自动选择使用有序队列或并发队列。
 *
 * ## 作用域所有权
 *
 * 本类不创建自己的根作用域，而由构造期注入一个宿主提供的应用级 [CoroutineScope]，
 * 通常来自已持有该作用域的组件。[shutdown] 仅取消本作用域的在途任务，
 * 绝不取消注入进来的共享作用域。
 *
 * 特性：
 * - 独立的生命周期管理
 * - 根据并发数自动选择队列类型
 * - 支持有序和并发两种模式
 * - 支持任务状态回调
 * - 简洁的 API 设计
 *
 * 使用示例：
 * ```kotlin
 * // 创建有序队列作用域
 * val serialScope = TaskQueueScope.serial(applicationScope, "my_serial")
 * serialScope.enqueue(
 *     block = { progress -> doTask1(progress) },
 *     onStateChange = { state -> handleState(state) }
 * )
 *
 * // 创建并发队列作用域
 * val concurrentScope = TaskQueueScope.concurrent(applicationScope, "my_concurrent", maxConcurrency = 8)
 * concurrentScope.enqueue(
 *     block = { progress -> downloadImage(url1, progress) },
 *     onStateChange = { state -> handleState(state) }
 * )
 *
 * // 使用完毕后关闭
 * serialScope.shutdown()
 * concurrentScope.shutdown()
 * ```
 *
 * @param scope 宿主提供的应用级协程作用域，其生命周期应覆盖本作用域
 * @param name 队列作用域名称
 * @param maxConcurrency 最大并发数，1 表示有序队列，大于 1 表示并发队列
 *
 * @see SerialTaskQueue
 * @see ConcurrentTaskQueue
 * @see TaskState
 */
class TaskQueueScope(
    private val scope: CoroutineScope,
    private val name: String,
    private val maxConcurrency: Int = 1
) {

    init { require(maxConcurrency > 0) { "maxConcurrency must be positive" } }

    /**
     * 有序任务队列
     *
     * 当 [maxConcurrency] 为 1 时使用此队列。
     */
    private val serialQueue: SerialTaskQueue? by lazy {
        if (maxConcurrency == 1) SerialTaskQueue(scope) else null
    }

    /**
     * 并发任务队列
     *
     * 当 [maxConcurrency] 大于 1 时使用此队列。
     */
    private val concurrentQueue: ConcurrentTaskQueue? by lazy {
        if (maxConcurrency > 1) ConcurrentTaskQueue(scope, maxConcurrency) else null
    }

    /**
     * 队列是否空闲
     *
     * 当队列中没有待执行任务且没有正在执行的任务时返回 true。
     *
     * @return 如果队列为空闲状态则返回 true
     */
    val isIdle: Boolean
        get() = serialQueue?.isIdle ?: concurrentQueue?.isIdle ?: true

    /**
     * 队列状态流
     *
     * 可观察的任务队列状态，用于监控队列的运行情况。
     * 返回类型取决于队列类型（有序或并发）。
     */
    val state: StateFlow<*>?
        get() = serialQueue?.state ?: concurrentQueue?.state

    /**
     * 将任务加入队列
     *
     * 将指定的任务添加到队列中执行。
     *
     * @param T 任务返回值类型
     * @param task 要执行的任务
     * @return 加入队列的任务实例
     */
    fun <T> enqueue(task: Task<T>): Task<T> {
        return if (maxConcurrency == 1) {
            serialQueue!!.enqueue(task)
        } else {
            concurrentQueue!!.enqueue(task)
        }
    }

    /**
     * 创建并加入任务（带进度报告）
     *
     * 便捷方法，创建一个新任务并加入队列。
     *
     * @param T 任务返回值类型
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数，接收进度报告器
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并加入队列的任务实例
     */
    fun <T> enqueue(
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend (ProgressReporter) -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        return if (maxConcurrency == 1) {
            serialQueue!!.enqueue(priority, block, onSuccess, onError, onStateChange)
        } else {
            concurrentQueue!!.enqueue(priority, block, onSuccess, onError, onStateChange)
        }
    }

    /**
     * 创建并加入任务（简化版本，无进度报告）
     *
     * @param T 任务返回值类型
     * @param priority 任务优先级，默认为 [TaskPriority.NORMAL]
     * @param block 任务执行的挂起函数
     * @param onSuccess 成功回调，可选
     * @param onError 失败回调，可选
     * @param onStateChange 状态变化回调，可选
     * @return 创建并加入队列的任务实例
     */
    fun <T> enqueue(
        priority: TaskPriority = TaskPriority.NORMAL,
        block: suspend () -> T,
        onSuccess: ((T) -> Unit)? = null,
        onError: ((Throwable) -> Unit)? = null,
        onStateChange: ((TaskState) -> Unit)? = null
    ): Task<T> {
        return if (maxConcurrency == 1) {
            serialQueue!!.enqueue(priority, block, onSuccess, onError, onStateChange)
        } else {
            concurrentQueue!!.enqueue(priority, block, onSuccess, onError, onStateChange)
        }
    }

    /**
     * 取消指定任务
     *
     * 取消指定ID的任务。
     *
     * @param taskId 要取消的任务ID
     * @return 如果成功取消任务则返回 true，否则返回 false
     */
    fun cancel(taskId: String): Boolean {
        return serialQueue?.cancel(taskId) ?: concurrentQueue?.cancel(taskId) ?: false
    }

    /**
     * 取消所有任务
     *
     * 取消队列中所有待执行的任务和正在执行的任务。
     */
    fun cancelAll() {
        serialQueue?.cancelAll()
        concurrentQueue?.cancelAll()
    }

    /**
     * 关闭队列作用域
     *
     * 取消所有任务并释放资源。**不会**取消通过构造期注入的共享作用域，
     * 以避免误伤应用级协程。关闭后不应再使用此作用域。
     */
    fun shutdown() {
        serialQueue?.shutdown()
        concurrentQueue?.shutdown()
    }

    companion object {
        /**
         * 创建有序队列作用域
         *
         * 工厂方法，创建一个有序执行任务的队列作用域。
         * 任务将按照优先级顺序依次执行。
         *
         * @param scope 宿主提供的应用级协程作用域，其生命周期应覆盖本作用域
         * @param name 队列名称，默认为 "default"
         * @return 新创建的有序队列作用域
         *
         * 使用示例：
         * ```kotlin
         * val scope = TaskQueueScope.serial(applicationScope, "my_serial")
         * scope.enqueue(
         *     block = { progress -> task1(progress) },
         *     onStateChange = { state -> handleState(state) }
         * )
         * ```
         */
        fun serial(
            scope: CoroutineScope,
            name: String = "default"
        ): TaskQueueScope {
            return TaskQueueScope(scope, name, maxConcurrency = 1)
        }

        /**
         * 创建并发队列作用域
         *
         * 工厂方法，创建一个并发执行任务的队列作用域。
         * 多个任务可以同时执行，并发数受 [maxConcurrency] 限制。
         *
         * @param scope 宿主提供的应用级协程作用域，其生命周期应覆盖本作用域
         * @param name 队列名称，默认为 "default"
         * @param maxConcurrency 最大并发数，默认为 [ConcurrentTaskQueue.DEFAULT_CONCURRENCY]
         * @return 新创建的并发队列作用域
         *
         * 使用示例：
         * ```kotlin
         * val scope = TaskQueueScope.concurrent(applicationScope, "my_concurrent", maxConcurrency = 8)
         * scope.enqueue(
         *     block = { progress -> downloadImage(url, progress) },
         *     onStateChange = { state -> handleState(state) }
         * )
         * ```
         */
        fun concurrent(
            scope: CoroutineScope,
            name: String = "default",
            maxConcurrency: Int = ConcurrentTaskQueue.DEFAULT_CONCURRENCY
        ): TaskQueueScope {
            return TaskQueueScope(scope, name, maxConcurrency)
        }
    }
}
