package com.cn.tasks

import com.cn.tasks.log.TaskLog
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

/** 标记本队列的同步通知与任务执行，防止满容量 BLOCK 重入等待自身推进。 */
internal class QueueExecutionContext : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<QueueExecutionContext>

    private val entered = ThreadLocal<Boolean>()

    val isEntered: Boolean get() = entered.get() == true

    /** 同时携带队列身份与线程标记，供进度报告器在外部线程恢复同步通知保护。 */
    val coroutineContext: CoroutineContext get() = this + entered.asContextElement(true)

    fun <T> notify(action: () -> T): T {
        val previous = entered.get()
        entered.set(true)
        return try {
            action()
        } finally {
            if (previous == null) entered.remove() else entered.set(previous)
        }
    }

    /** 前置通知失败后仍刷新已变更集合的等待状态；两阶段失败按既有优先级合并。 */
    fun notifyAndRefresh(action: () -> Unit, refresh: () -> Unit) {
        notify {
            var failure: Throwable? = null
            try {
                action()
            } catch (error: Throwable) {
                failure = error
            }
            try {
                refresh()
            } catch (error: Throwable) {
                failure = failure?.let { combineTaskFailures(it, error) } ?: error
            }
            failure?.let { throw it }
        }
    }

    /** 聚合普通通知异常后继续整批更新；致命错误立即传播，不归入可隔离的取消异常。 */
    fun notifyAll(actions: List<() -> Unit>) = notify {
        var failure: Throwable? = null
        actions.forEach { action ->
            try {
                action()
            } catch (error: Exception) {
                if (failure == null) failure = error else if (failure !== error) failure!!.addSuppressed(error)
            }
        }
        failure?.let { throw it }
    }

    /** 通知自己的取消异常不取消已出队任务；调度协程的真实取消仍向上传播。 */
    suspend fun notifyForDispatch(action: () -> Unit) {
        try {
            notify(action)
        } catch (error: CancellationException) {
            currentCoroutineContext().ensureActive()
            TaskLog.e("Pending task notification failed", "TaskQueue", error)
        }
    }
}
