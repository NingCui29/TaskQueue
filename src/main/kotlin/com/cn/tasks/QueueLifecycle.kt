package com.cn.tasks

import com.cn.tasks.log.TaskLog
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job

/** 关闭属于生命周期拒绝，不能误报为容量已满。 */
class TaskQueueClosedException(val queueName: String) :
    IllegalStateException("Task queue '$queueName' is closed")

// 必须在 owner 开始取消时解除 BLOCK 等待，而非等待其 NonCancellable 子任务清理结束。
// 观察句柄不会作为子 Job 阻止 owner 正常完成，队列主动关闭时会注销它。
@OptIn(InternalCoroutinesApi::class)
internal fun observeOwnerCancellation(scope: CoroutineScope, onClosed: () -> Unit): DisposableHandle? =
    scope.coroutineContext[Job]?.invokeOnCompletion(onCancelling = true, invokeImmediately = true) {
        try {
            onClosed()
        } catch (error: Throwable) {
            // 外部状态回调失败不应中断 owner 对其他组件的取消传播。
            TaskLog.e("Queue owner cancellation callback failed", "TaskQueue", error)
        }
    }

internal fun cancelOwnedTasks(jobs: List<Job>, tasks: List<Task<*>>) {
    var failure: Throwable? = null
    val actions = jobs.map { job -> { job.cancel() } } + tasks.map { task -> { task.updateState(TaskState.Cancelled) } }
    actions.forEach { action ->
        try {
            action()
        } catch (error: Throwable) {
            failure = failure?.let { combineTaskFailures(it, error) } ?: error
        }
    }
    failure?.let { throw it }
}

internal fun closedRejection(
    task: Task<*>,
    queueSize: Int,
    maxCapacity: Int,
    onRejected: ((Task<*>, String) -> Unit)?,
): EnqueueResult.Rejected {
    val reason = "Queue is closed"
    task.cancel()
    task.updateState(TaskState.Cancelled)
    onRejected?.invoke(task, reason)
    return EnqueueResult.Rejected(task, reason, queueSize, maxCapacity, EnqueueResult.RejectionCause.CLOSED)
}

/** 合并执行与退出失败：致命错误优先，同等级保留首个错误，其余作为附加异常。 */
internal fun combineTaskFailures(primary: Throwable, secondary: Throwable): Throwable {
    if (primary === secondary) return primary
    val preferred = if (primary is Exception && secondary !is Exception) secondary else primary
    preferred.addSuppressed(if (preferred === primary) secondary else primary)
    return preferred
}
