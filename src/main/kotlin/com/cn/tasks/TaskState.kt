package com.cn.tasks

/**
 * 任务状态。
 *
 * 状态流转：
 * ```
 * Pending ──▶ Running ──▶ Completed
 *    │           │
 *    └───────────┴──────▶ Cancelled
 * ```
 *
 * ## 相对旧 `com.cn.core.task.TaskState` 的调整
 *
 * 由 `sealed class` 改为 `sealed interface`：子类型不再继承一个空的抽象类，
 * 少一层类层级；`when` 穷尽性检查行为完全一致。
 *
 * @see Task
 * @see TaskCallback
 */
sealed interface TaskState {

    /**
     * 等待中：任务已入队，尚未开始执行。
     *
     * @param queuePosition 在队列中的执行位次，从 1 开始；[UNKNOWN_POSITION] 表示未知
     * @param queueSize 队列中待执行任务总数
     */
    data class Pending(
        val queuePosition: Int = UNKNOWN_POSITION,
        val queueSize: Int = 0,
    ) : TaskState

    /**
     * 执行中。
     *
     * @param progress 执行进度，取值 0.0 ~ 1.0
     * @param message 进度描述，可选
     */
    data class Running(
        val progress: Float = 0f,
        val message: String? = null,
    ) : TaskState

    /**
     * 已完成（含失败完成）。
     *
     * @param success 是否成功
     * @param error 失败时的异常；成功时为 null
     */
    data class Completed(
        val success: Boolean,
        val error: Throwable? = null,
    ) : TaskState

    /** 已取消。 */
    data object Cancelled : TaskState

    companion object {
        /** [Pending.queuePosition] 的未知值。 */
        const val UNKNOWN_POSITION: Int = -1
    }
}

/**
 * 进度报告器。
 *
 * 任务体 `block` 的唯一入参，用于在执行过程中回报进度。
 *
 * ```kotlin
 * queue.enqueue(block = { progress ->
 *     repeat(100) { i ->
 *         progress.report((i + 1) / 100f, "处理第 ${i + 1} 项")
 *     }
 * })
 * ```
 */
interface ProgressReporter {
    /**
     * 报告进度。传入值会被裁剪到 `0f..1f`，NaN 被忽略。
     * 仅在本次任务体仍有效时交付；任务体退出或取消后的迟到报告被忽略。
     * 可从其他线程报告，回调仍在报告线程执行，并保留所属队列的同步通知重入保护。
     *
     * @param progress 进度值
     * @param message 进度描述，可选
     */
    fun report(progress: Float, message: String? = null)
}

/**
 * 任务回调聚合接口。
 *
 * 相比逐个传 lambda，实现该接口可把一组回调收敛到一个对象里，
 * 通过 [TaskBuilder.callback] 一次性挂载。
 *
 * ## 相对旧实现的修正
 *
 * 旧 `TaskCallback` 定义了却从未被 `Task` 使用，属于死 API；
 * 本实现通过 [TaskBuilder.callback] 将其接入构建链路。
 *
 * @param T 任务返回值类型
 */
interface TaskCallback<T> {

    /** 状态变化。 */
    fun onStateChanged(state: TaskState) {}

    /** 执行成功。 */
    fun onSuccess(result: T) {}

    /** 执行失败（含超时）。 */
    fun onError(error: Throwable) {}

    /** 执行结束，无论成功、失败还是取消都会回调。 */
    fun onComplete() {}
}

/**
 * [TaskCallback] 的空实现，便于 Java 侧或只关心部分回调的场景按需重写。
 *
 * Kotlin 侧可直接实现 [TaskCallback]——接口方法已带默认实现，无需该适配器。
 */
open class TaskCallbackAdapter<T> : TaskCallback<T>
