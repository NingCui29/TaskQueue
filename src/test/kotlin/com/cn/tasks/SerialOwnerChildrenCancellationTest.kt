package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** 验证所有者仅取消子协程时，串行队列持续持有清理任务并恢复后续调度。 */
@OptIn(ExperimentalCoroutinesApi::class)
class SerialOwnerChildrenCancellationTest {

    @Test
    fun acceptedPendingWorkResumesAfterOwnerChildrenCancellationAndCleanup() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val ownerJob = SupervisorJob()
        val owner = CoroutineScope(ownerJob + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        var cleanupFinished = false
        var pendingRan = false
        val first = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                    cleanupFinished = true
                }
            }
        })
        val pending = Task(block = { pendingRan = true })

        try {
            queue.enqueue(first)
            queue.enqueue(pending)
            runCurrent()
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, queue.pendingCount)

            ownerJob.cancelChildren()
            runCurrent()

            assertTrue(cleaning)
            assertFalse(cleanupFinished)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(1, queue.pendingCount)
            assertFalse(pendingRan)
            assertTrue(pending.currentState is TaskState.Pending)
            assertFalse(queue.isIdle)
            assertFalse(queue.isClosed)
            assertTrue(owner.isActive)

            releaseCleanup.complete(Unit)
            runCurrent()

            assertTrue(cleanupFinished)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertTrue(pendingRan)
            assertEquals(TaskState.Completed(true), pending.currentState)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    @Test
    fun newSubmissionsCannotReplaceAnOlderTasksCleanupOwnership() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val ownerJob = SupervisorJob()
        val owner = CoroutineScope(ownerJob + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseCleanup = CompletableDeferred<Unit>()
        val events = mutableListOf<String>()
        var cleaning = false
        val first = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                    events += "cleanup"
                }
            }
        })

        try {
            queue.enqueue(first)
            runCurrent()
            ownerJob.cancelChildren()
            runCurrent()
            assertTrue(cleaning)

            val second = queue.enqueue(Task(block = { events += "second" }))
            val third = queue.enqueue(Task(block = { events += "third" }))
            runCurrent()

            assertTrue(second.currentState is TaskState.Pending)
            assertTrue(third.currentState is TaskState.Pending)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(2, queue.pendingCount)
            assertTrue(events.isEmpty())
            assertFalse(queue.isIdle)
            assertFalse(releaseCleanup.isCompleted)

            releaseCleanup.complete(Unit)
            runCurrent()

            assertEquals(listOf("cleanup", "second", "third"), events)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertEquals(TaskState.Completed(true), second.currentState)
            assertEquals(TaskState.Completed(true), third.currentState)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    @Test
    fun shutdownWaitsForCleanupAfterOwnerChildrenAreCancelled() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val ownerJob = SupervisorJob()
        val owner = CoroutineScope(ownerJob + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        var cleanupFinished = false
        var pendingRan = false
        val first = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                    cleanupFinished = true
                }
            }
        })

        try {
            queue.enqueue(first)
            runCurrent()
            ownerJob.cancelChildren()
            runCurrent()
            assertTrue(cleaning)

            val pending = queue.enqueue(Task(block = { pendingRan = true }))
            runCurrent()
            queue.shutdown()
            runCurrent()

            assertEquals(TaskState.Cancelled, pending.currentState)
            assertFalse(pendingRan)
            assertFalse(cleanupFinished)
            assertEquals(1, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertTrue(queue.isClosed)
            assertFalse(queue.isTerminated)
            assertFalse(queue.isIdle)
            assertEquals(SerialTaskQueue.QueueState.Closing(1), queue.state.value)
            assertTrue(owner.isActive)

            releaseCleanup.complete(Unit)
            runCurrent()

            assertTrue(cleanupFinished)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertFalse(pendingRan)
            assertEquals(0, queue.runningTaskCount)
            assertTrue(queue.isTerminated)
            assertTrue(queue.isIdle)
            assertEquals(SerialTaskQueue.QueueState.Shutdown, queue.state.value)
            assertTrue(owner.isActive)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    @Test
    fun cleanupCompletionHonorsPauseUntilAnExplicitResume() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val ownerJob = SupervisorJob()
        val owner = CoroutineScope(ownerJob + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cleaning = false
        var pendingRan = false
        val first = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    cleaning = true
                    releaseCleanup.await()
                }
            }
        })
        val pending = Task(block = { pendingRan = true })

        try {
            queue.enqueue(first)
            queue.enqueue(pending)
            runCurrent()
            ownerJob.cancelChildren()
            runCurrent()
            assertTrue(cleaning)

            queue.pause()
            releaseCleanup.complete(Unit)
            runCurrent()

            assertEquals(TaskState.Cancelled, first.currentState)
            assertFalse(pendingRan)
            assertTrue(pending.currentState is TaskState.Pending)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(1, queue.pendingCount)
            assertEquals(SerialTaskQueue.QueueState.Paused(1), queue.state.value)
            assertTrue(owner.isActive)

            queue.resume()
            runCurrent()

            assertTrue(pendingRan)
            assertEquals(TaskState.Completed(true), pending.currentState)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertTrue(queue.isIdle)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }
}
