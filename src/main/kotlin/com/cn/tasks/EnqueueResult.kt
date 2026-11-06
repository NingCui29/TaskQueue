package com.cn.tasks

/**
 * 入队结果。
 *
 * 描述一次入队操作在背压策略作用下的最终归宿。
 *
 * ```kotlin
 * when (val result = queue.enqueueWithResult(task)) {
 *     is EnqueueResult.Success -> TaskLog.d("已入队: ${result.task.id}", TAG)
 *     is EnqueueResult.Rejected -> TaskLog.w("被拒绝: ${result.reason}", TAG)
 *     is EnqueueResult.Discarded -> TaskLog.w("淘汰旧任务: ${result.discardedTask.name}", TAG)
 *     is EnqueueResult.CallerExecuted -> TaskLog.d("已在调用者线程执行", TAG)
 * }
 * ```
 *
 * ## 相对旧 `com.cn.core.task.EnqueueResult` 的调整
 *
 * 1. `sealed class` 改为 `sealed interface`，去掉一层空抽象基类。
 * 2. 修正 [isSuccess] 语义：[Discarded] 表示"新任务已入队、仅淘汰了旧任务"，
 *    属于入队成功，旧实现却将其判为不成功，与其自身 KDoc 相互矛盾。
 * 3. 新增 [isDiscarded]，与 [isRejected] 对齐，避免调用方写 `is` 判断。
 *
 * @see RejectionPolicy
 * @see SerialTaskQueue
 * @see ConcurrentTaskQueue
 */
sealed interface EnqueueResult {

    /** 入队的任务；[Discarded] 场景下指新任务。 */
    val task: Task<*>

    /**
     * 入队成功。
     *
     * @param task 成功入队的任务
     */
    data class Success(override val task: Task<*>) : EnqueueResult

    /**
     * 任务被拒绝。
     *
     * 对应队列关闭或所属协程作用域结束，以及 DISCARD、THROW_EXCEPTION 和满容量 BLOCK
     * 同队列执行/通知重入的容量拒绝；[cause] 区分生命周期与容量。
     *
     * @param task 被拒绝的任务
     * @param reason 拒绝原因
     * @param queueSize 触发时的队列大小
     * @param maxCapacity 队列最大容量
     */
    data class Rejected(
        override val task: Task<*>,
        val reason: String,
        val queueSize: Int,
        val maxCapacity: Int,
        val cause: RejectionCause = RejectionCause.CAPACITY,
    ) : EnqueueResult

    /** 可判断的拒绝分类；关闭独立于队列满时的背压策略。 */
    enum class RejectionCause { CAPACITY, CLOSED }

    /**
     * 新任务入队成功，同时淘汰了一个旧任务。
     *
     * 对应 [RejectionPolicy.DISCARD_OLDEST]。
     *
     * @param task 新入队的任务
     * @param discardedTask 被淘汰的旧任务
     */
    data class Discarded(
        override val task: Task<*>,
        val discardedTask: Task<*>,
    ) : EnqueueResult

    /**
     * 任务已在调用者线程同步执行完毕。
     *
     * 对应 [RejectionPolicy.CALLER_RUNS]。
     *
     * @param task 已执行的任务
     * @param success 是否执行成功
     * @param error 失败时的异常；成功时为 null
     */
    data class CallerExecuted(
        override val task: Task<*>,
        val success: Boolean,
        val error: Throwable? = null,
    ) : EnqueueResult

    /**
     * 任务是否被队列接纳（入队或就地执行）。
     *
     * [Discarded] 计为成功——新任务确实进入了队列。
     */
    val isSuccess: Boolean
        get() = this is Success || this is Discarded || this is CallerExecuted

    /** 任务是否被拒绝，即未被执行也未入队。 */
    val isRejected: Boolean
        get() = this is Rejected

    /** 是否因容量不足淘汰了旧任务。 */
    val isDiscarded: Boolean
        get() = this is Discarded
}
