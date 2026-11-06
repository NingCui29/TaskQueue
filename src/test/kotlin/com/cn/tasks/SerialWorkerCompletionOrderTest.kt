package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** 分别推进任务与工作循环，验证任务先完成取消时仍能恰好恢复一次串行调度。 */
@OptIn(ExperimentalCoroutinesApi::class)
class SerialWorkerCompletionOrderTest {

    @Test
    fun pendingWorkResumesSeriallyWhenTaskCancellationCompletesBeforeTheWorker() {
        val workerScheduler = TestCoroutineScheduler()
        val taskScheduler = TestCoroutineScheduler()
        val ownerJob = SupervisorJob()
        val owner = CoroutineScope(ownerJob + StandardTestDispatcher(workerScheduler))
        val queue = SerialTaskQueue(owner, StandardTestDispatcher(taskScheduler))
        val releasePending = CompletableDeferred<Unit>()
        val events = mutableListOf<String>()
        val first = Task<Unit>(block = {
            try {
                events += "first-start"
                awaitCancellation()
            } finally {
                events += "first-cleanup"
            }
        })
        val pending = Task(block = {
            events += "pending-start"
            releasePending.await()
            events += "pending-end"
        })
        val last = Task(block = { events += "last" })

        try {
            queue.enqueue(first)
            val oldWorker = ownerJob.children.single()
            queue.enqueue(pending)
            workerScheduler.runCurrent()
            val firstJob = ownerJob.children.single { it !== oldWorker }
            taskScheduler.runCurrent()
            assertEquals(listOf("first-start"), events)

            ownerJob.cancelChildren()
            taskScheduler.runCurrent()

            assertTrue(firstJob.isCompleted)
            assertFalse("The worker cancellation has not been dispatched yet", oldWorker.isCompleted)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertEquals(listOf("first-start", "first-cleanup"), events)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(1, queue.pendingCount)
            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            assertTrue(owner.isActive)
            assertFalse(queue.isClosed)

            queue.enqueue(last)
            taskScheduler.runCurrent()
            assertEquals("No replacement worker may start before the old worker completes", listOf(oldWorker), ownerJob.children.toList())
            assertTrue(pending.currentState is TaskState.Pending)
            assertTrue(last.currentState is TaskState.Pending)

            workerScheduler.runCurrent()
            taskScheduler.runCurrent()

            assertTrue(oldWorker.isCompleted)
            assertEquals(listOf("first-start", "first-cleanup", "pending-start"), events)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, queue.pendingCount)
            assertTrue(last.currentState is TaskState.Pending)

            releasePending.complete(Unit)
            taskScheduler.runCurrent()
            workerScheduler.runCurrent()
            taskScheduler.runCurrent()
            workerScheduler.runCurrent()

            assertEquals(listOf("first-start", "first-cleanup", "pending-start", "pending-end", "last"), events)
            assertEquals(TaskState.Completed(true), pending.currentState)
            assertEquals(TaskState.Completed(true), last.currentState)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            releasePending.complete(Unit)
            queue.shutdown()
            owner.cancel()
            taskScheduler.runCurrent()
            workerScheduler.runCurrent()
            taskScheduler.runCurrent()
            workerScheduler.runCurrent()
        }
    }
}
