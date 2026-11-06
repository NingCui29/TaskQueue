package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** 验证通知正常返回后的真实派发取消，以及保留任务的所有权与许可回收。 */
@OptIn(ExperimentalCoroutinesApi::class)
class QueueDispatchCancellationTest {

    @Test
    fun pendingNotificationCancellationPreventsQueuedBusinessExecution() = runTest {
        verifyCancelledDispatch(StandardTestDispatcher(testScheduler), cancelFromQueueState = false)
    }

    @Test
    fun pendingNotificationCancellationPreventsImmediateBusinessExecution() = runTest {
        verifyCancelledDispatch(Dispatchers.Unconfined, cancelFromQueueState = false)
    }

    @Test
    fun queueStateNotificationCancellationPreventsQueuedBusinessExecution() = runTest {
        verifyCancelledDispatch(StandardTestDispatcher(testScheduler), cancelFromQueueState = true)
    }

    @Test
    fun queueStateNotificationCancellationPreventsImmediateBusinessExecution() = runTest {
        verifyCancelledDispatch(Dispatchers.Unconfined, cancelFromQueueState = true)
    }

    @Test
    fun cancellingWorkerAfterImmediateTaskCompletionPreservesTheNextPendingTask() = runTest {
        for (concurrent in listOf(false, true)) {
            val ownerJob = SupervisorJob()
            val owner = CoroutineScope(ownerJob + StandardTestDispatcher(testScheduler))
            val serialQueue = if (concurrent) null else SerialTaskQueue(owner, Dispatchers.Unconfined)
            val concurrentQueue = if (concurrent) ConcurrentTaskQueue(owner, 1, Dispatchers.Unconfined) else null
            fun enqueue(task: Task<Unit>) {
                if (serialQueue != null) serialQueue.enqueue(task) else concurrentQueue!!.enqueue(task)
            }
            val scenario = "concurrent=$concurrent"
            lateinit var worker: Job
            val events = mutableListOf<String>()
            var firstCompletions = 0
            var pendingCancellations = 0
            val first = Task<Unit>(
                block = { events += "first" },
                onComplete = {
                    firstCompletions++
                    worker.cancel()
                },
            )
            val pending = Task<Unit>(
                block = { events += "pending" },
                onStateChange = { if (it is TaskState.Cancelled) pendingCancellations++ },
            )

            try {
                enqueue(first)
                worker = ownerJob.children.single()
                enqueue(pending)
                runCurrent()

                // 已取消的旧循环不能再取出下一项；未派发任务应由新循环继续处理。
                assertTrue(scenario, worker.isCancelled)
                assertTrue(scenario, worker.isCompleted)
                assertEquals(scenario, listOf("first", "pending"), events)
                assertEquals(scenario, 1, firstCompletions)
                assertEquals(scenario, 0, pendingCancellations)
                assertEquals(scenario, TaskState.Completed(true), first.currentState)
                assertEquals(scenario, TaskState.Completed(true), pending.currentState)
                assertFalse(scenario, first.isCancelled)
                assertFalse(scenario, pending.isCancelled)
                assertEquals(scenario, 0, serialQueue?.runningTaskCount ?: concurrentQueue!!.runningTaskCount)
                assertEquals(scenario, 0, serialQueue?.pendingCount ?: concurrentQueue!!.pendingTaskCount)
                assertTrue(scenario, serialQueue?.isIdle ?: concurrentQueue!!.isIdle)
                assertTrue(scenario, owner.isActive)
            } finally {
                if (serialQueue != null) serialQueue.shutdown() else concurrentQueue!!.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    private fun TestScope.verifyCancelledDispatch(
        taskDispatcher: CoroutineDispatcher,
        cancelFromQueueState: Boolean,
    ) {
        for (concurrent in listOf(false, true)) {
            for (cancelOwnerChildren in listOf(false, true)) {
                val scenario = "concurrent=$concurrent queueState=$cancelFromQueueState ownerChildren=$cancelOwnerChildren"
                val ownerJob = SupervisorJob()
                val owner = CoroutineScope(ownerJob + StandardTestDispatcher(testScheduler))
                val serialQueue = if (concurrent) null else SerialTaskQueue(owner, taskDispatcher)
                val concurrentQueue = if (concurrent) ConcurrentTaskQueue(owner, 1, taskDispatcher) else null
                fun enqueue(task: Task<Unit>) {
                    if (serialQueue != null) serialQueue.enqueue(task) else concurrentQueue!!.enqueue(task)
                }
                fun shutdown() {
                    if (serialQueue != null) serialQueue.shutdown() else concurrentQueue!!.shutdown()
                }
                fun runningCount() = serialQueue?.runningTaskCount ?: concurrentQueue!!.runningTaskCount
                fun pendingCount() = serialQueue?.pendingCount ?: concurrentQueue!!.pendingTaskCount
                fun isIdle() = serialQueue?.isIdle ?: concurrentQueue!!.isIdle
                fun isClosed() = serialQueue?.isClosed ?: concurrentQueue!!.isClosed

                lateinit var worker: Job
                var armed = false
                var cancelledDuringDispatch = false
                var reservedRuns = 0
                var cancellations = 0
                var completions = 0
                val events = mutableListOf<String>()
                val releaseReplacement = CompletableDeferred<Unit>()
                fun cancelWorker() {
                    cancelledDuringDispatch = true
                    if (cancelOwnerChildren) ownerJob.cancelChildren() else worker.cancel()
                    // 不抛通知异常，直接验证已取消的派发不能继续启动业务。
                    assertTrue(scenario, worker.isCancelled)
                }
                val reserved = Task<Unit>(
                    block = { reservedRuns++ },
                    onStateChange = { if (it is TaskState.Cancelled) cancellations++ },
                    onComplete = { completions++ },
                )
                val remaining = Task<Unit>(
                    block = { events += "remaining" },
                    onStateChange = { state ->
                        if (!cancelFromQueueState && armed && !cancelledDuringDispatch &&
                            state is TaskState.Pending && state.queueSize == 1
                        ) {
                            cancelWorker()
                        }
                    },
                )
                val observer = if (!cancelFromQueueState) null else backgroundScope.launch(Dispatchers.Unconfined) {
                    if (serialQueue != null) {
                        serialQueue.state.collect { state ->
                            if (armed && !cancelledDuringDispatch &&
                                state == SerialTaskQueue.QueueState.Running(reserved.name)
                            ) {
                                cancelWorker()
                            }
                        }
                    } else {
                        concurrentQueue!!.state.collect { state ->
                            if (armed && !cancelledDuringDispatch &&
                                state == ConcurrentTaskQueue.ConcurrentState.Running(1, 1)
                            ) {
                                cancelWorker()
                            }
                        }
                    }
                }

                try {
                    enqueue(reserved)
                    worker = ownerJob.children.single()
                    enqueue(remaining)
                    armed = true
                    runCurrent()

                    assertTrue(scenario, cancelledDuringDispatch)
                    assertTrue(scenario, worker.isCancelled)
                    assertTrue(scenario, owner.isActive)
                    assertFalse(scenario, isClosed())
                    assertEquals(scenario, 0, reservedRuns)
                    assertEquals(scenario, TaskState.Cancelled, reserved.currentState)
                    assertEquals(scenario, 1, cancellations)
                    assertEquals("The reservation never entered execute: $scenario", 0, completions)
                    assertEquals(scenario, listOf("remaining"), events)
                    assertEquals(scenario, TaskState.Completed(true), remaining.currentState)
                    assertEquals(scenario, 0, runningCount())
                    assertEquals(scenario, 0, pendingCount())
                    assertTrue(scenario, isIdle())

                    // 同 ID 可重新接纳，且等待门未释放前不能多还许可或越过串行限制。
                    val replacement = Task<Unit>(id = reserved.id, block = {
                        events += "replacement-start"
                        releaseReplacement.await()
                        events += "replacement-end"
                    })
                    val following = Task<Unit>(block = { events += "following" })
                    enqueue(replacement)
                    enqueue(following)
                    runCurrent()

                    assertEquals(scenario, listOf("remaining", "replacement-start"), events)
                    assertTrue(scenario, replacement.currentState is TaskState.Running)
                    assertTrue(scenario, following.currentState is TaskState.Pending)
                    assertEquals(scenario, 1, runningCount())
                    assertEquals(scenario, 1, pendingCount())
                    assertFalse(scenario, isIdle())

                    releaseReplacement.complete(Unit)
                    runCurrent()

                    assertEquals(scenario, listOf("remaining", "replacement-start", "replacement-end", "following"), events)
                    assertEquals(scenario, TaskState.Completed(true), replacement.currentState)
                    assertEquals(scenario, TaskState.Completed(true), following.currentState)
                    assertEquals(scenario, 0, runningCount())
                    assertEquals(scenario, 0, pendingCount())
                    assertTrue(scenario, isIdle())
                    assertTrue(scenario, owner.isActive)
                } finally {
                    releaseReplacement.complete(Unit)
                    observer?.cancel()
                    shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }
}
