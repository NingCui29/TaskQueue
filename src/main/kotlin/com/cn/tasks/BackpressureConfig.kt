package com.cn.tasks

/**
 * 背压配置。
 *
 * 描述任务队列的容量上限、超限后的处置策略，以及三类观测回调。
 *
 * ```kotlin
 * // 容量 100，超限淘汰最旧任务
 * val config = BackpressureConfig.discardOldest(maxCapacity = 100)
 *
 * // 自定义 + 观测回调
 * val config = BackpressureConfig(
 *     maxCapacity = 50,
 *     policy = RejectionPolicy.DISCARD,
 *     onRejected = { task, reason -> TaskLog.w("任务被拒绝: ${task.name}, $reason", TAG) },
 * )
 * ```
 *
 * ## 相对旧 `com.cn.core.task.BackpressureConfig` 的调整
 *
 * 1. 使用率计算改用 `Long` 中间量。旧实现 `(currentSize * 100) / maxCapacity` 在
 *    `currentSize > 21_474_836` 时会发生 `Int` 溢出并得到负数百分比，
 *    进而使 [isThresholdReached] 恒假、警告失效。
 * 2. [isThresholdReached] 复用 [calculateUsagePercent]，消除同一算式的两处副本。
 * 3. 补齐 [block] 工厂方法。旧实现为 5 个策略中的 4 个提供了工厂，
 *    唯独遗漏 [RejectionPolicy.BLOCK]，API 面不完整。
 *
 * ## 关于 `data class` 与回调相等性
 *
 * 本类保留 `data class` 以提供 `copy()`。注意其 `equals`/`hashCode` 会纳入三个函数类型
 * 属性，而函数对象按引用比较——两份配置即便策略与容量完全相同，只要各自传入了 lambda
 * 就不相等。该类型仅用于配置传递，不应作为 Map 键或参与去重。
 *
 * @param maxCapacity 等待任务容量（不含运行任务），默认 [DEFAULT_CAPACITY]；取 [UNBOUNDED] 表示不限容
 * @param policy 拒绝策略，默认 [RejectionPolicy.DISCARD_OLDEST]
 * @param onRejected 任务被拒绝时回调，参数为被拒任务与原因
 * @param onDiscarded 淘汰旧任务时回调，参数为被淘汰任务与新任务
 * @param warnThreshold 警告阈值百分比，取值 0..100，默认 [DEFAULT_WARN_THRESHOLD]
 * @param onThresholdReached 使用率达到 [warnThreshold] 时回调
 *
 * @see RejectionPolicy
 * @see SerialTaskQueue
 * @see ConcurrentTaskQueue
 */
data class BackpressureConfig(
    val maxCapacity: Int = DEFAULT_CAPACITY,
    val policy: RejectionPolicy = RejectionPolicy.DISCARD_OLDEST,
    val onRejected: ((task: Task<*>, reason: String) -> Unit)? = null,
    val onDiscarded: ((discardedTask: Task<*>, newTask: Task<*>) -> Unit)? = null,
    val warnThreshold: Int = DEFAULT_WARN_THRESHOLD,
    val onThresholdReached: ((currentSize: Int, maxCapacity: Int, usagePercent: Int) -> Unit)? = null,
) {

    init {
        require(maxCapacity > 0 || maxCapacity == UNBOUNDED) {
            "maxCapacity must be positive or UNBOUNDED, but was $maxCapacity"
        }
        require(warnThreshold in 0..100) {
            "warnThreshold must be in 0..100, but was $warnThreshold"
        }
    }

    /**
     * 是否启用背压控制。
     *
     * [maxCapacity] 为 [UNBOUNDED] 时不启用，队列退化为无界队列。
     */
    val isBackpressureEnabled: Boolean
        get() = maxCapacity != UNBOUNDED

    /**
     * 队列使用率是否已达 [warnThreshold]。
     *
     * @param currentSize 当前队列大小；负数按 0，超过容量按满容量处理
     * @return 无界队列恒为 false
     */
    fun isThresholdReached(currentSize: Int): Boolean =
        isBackpressureEnabled && calculateUsagePercent(currentSize) >= warnThreshold

    /**
     * 计算队列使用率百分比。
     *
     * @param currentSize 当前队列大小；负数按 0，超过容量按满容量处理
     * @return 0..100 的百分比；无界队列恒返回 0
     */
    fun calculateUsagePercent(currentSize: Int): Int {
        if (!isBackpressureEnabled) return 0
        // 先夹取业务范围，再以 Long 承载乘法，避免中间计算和转回 Int 时溢出。
        val boundedSize = currentSize.coerceIn(0, maxCapacity)
        return (boundedSize.toLong() * 100L / maxCapacity).toInt()
    }

    companion object {

        /** 无限制容量。取此值时队列不限大小，且不启用背压控制。 */
        const val UNBOUNDED: Int = Int.MAX_VALUE

        /** 默认队列容量。 */
        const val DEFAULT_CAPACITY: Int = 100

        /** 默认警告阈值，百分比。 */
        const val DEFAULT_WARN_THRESHOLD: Int = 80

        /** 无界配置：不限容量，不启用背压。 */
        fun unbounded(): BackpressureConfig =
            BackpressureConfig(maxCapacity = UNBOUNDED)

        /** [RejectionPolicy.DISCARD] 配置。 */
        fun discard(maxCapacity: Int = DEFAULT_CAPACITY): BackpressureConfig =
            BackpressureConfig(maxCapacity = maxCapacity, policy = RejectionPolicy.DISCARD)

        /** [RejectionPolicy.DISCARD_OLDEST] 配置。 */
        fun discardOldest(maxCapacity: Int = DEFAULT_CAPACITY): BackpressureConfig =
            BackpressureConfig(maxCapacity = maxCapacity, policy = RejectionPolicy.DISCARD_OLDEST)

        /** [RejectionPolicy.THROW_EXCEPTION] 配置。 */
        fun throwException(maxCapacity: Int = DEFAULT_CAPACITY): BackpressureConfig =
            BackpressureConfig(maxCapacity = maxCapacity, policy = RejectionPolicy.THROW_EXCEPTION)

        /** [RejectionPolicy.CALLER_RUNS] 配置。禁止在主线程入队。 */
        fun callerRuns(maxCapacity: Int = DEFAULT_CAPACITY): BackpressureConfig =
            BackpressureConfig(maxCapacity = maxCapacity, policy = RejectionPolicy.CALLER_RUNS)

        /** [RejectionPolicy.BLOCK] 配置。禁止在主线程入队；满容量的同队列执行/通知重入会被拒绝。 */
        fun block(maxCapacity: Int = DEFAULT_CAPACITY): BackpressureConfig =
            BackpressureConfig(maxCapacity = maxCapacity, policy = RejectionPolicy.BLOCK)
    }
}
