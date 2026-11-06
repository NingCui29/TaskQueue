package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SerialTaskQueueTest {
    @Test fun reentrantCancellationCoversPolledTaskAndPreservesNewSubmissions() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        var cancelOnPositionUpdate = false
        var newTaskCompleted = false
        val first = Task<Unit>(block = { fail("Polled task must be cancelled") })
        val second = Task<Unit>(
            block = { fail("Pending task must be cancelled") },
            onStateChange = { state ->
                if (cancelOnPositionUpdate && state is TaskState.Pending && state.queueSize == 2) {
                    cancelOnPositionUpdate = false
                    queue.cancelAll()
                    queue.enqueue(Task(block = { newTaskCompleted = true }))
                }
            },
        )
        val third = Task<Unit>(block = { fail("Old position snapshot must not restore this task") })
        queue.enqueue(first)
        queue.enqueue(second)
        queue.enqueue(third)
        cancelOnPositionUpdate = true

        advanceUntilIdle()

        assertEquals(TaskState.Cancelled, first.currentState)
        assertEquals(TaskState.Cancelled, second.currentState)
        assertEquals(TaskState.Cancelled, third.currentState)
        assertTrue(newTaskCompleted)
        assertTrue(queue.isIdle)
        assertTrue(isActive)
    }

    @Test fun pauseAndIndividualCancellationCanReachTaskDuringDispatchHandoff() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        var pauseOnPositionUpdate = false
        var secondCompleted = false
        val first = Task<Unit>(block = { fail("Task polled before pause must not execute") })
        val second = Task(
            block = { secondCompleted = true },
            onStateChange = { state ->
                if (pauseOnPositionUpdate && state is TaskState.Pending && state.queueSize == 1) {
                    pauseOnPositionUpdate = false
                    queue.pause()
                    assertTrue(queue.cancel(first.id))
                }
            },
        )
        queue.enqueue(first)
        queue.enqueue(second)
        pauseOnPositionUpdate = true

        runCurrent()

        assertEquals(TaskState.Cancelled, first.currentState)
        assertEquals(SerialTaskQueue.QueueState.Paused(1), queue.state.value)
        assertFalse(secondCompleted)
        queue.resume()
        advanceUntilIdle()
        assertTrue(secondCompleted)
        assertTrue(queue.isIdle)
    }

    @Test fun immediateTaskDispatcherRegistersJobBeforeRunningStateCallback() = runTest {
        val queue = SerialTaskQueue(this, UnconfinedTestDispatcher(testScheduler))
        var ranAfterPause = false
        var completionCount = 0
        val task = Task(
            block = { ranAfterPause = true },
            onStateChange = { state -> if (state is TaskState.Running) queue.pause() },
            onComplete = { completionCount++ },
        )
        queue.enqueue(task)

        runCurrent()

        assertEquals(TaskState.Cancelled, task.currentState)
        assertFalse(ranAfterPause)
        assertEquals(1, completionCount)
        assertTrue(queue.isPaused)
        assertTrue(isActive)
    }

    @Test fun cancelledOwnerBeforeWorkerBodyStartsDoesNotLeaveRunningFlagSet() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val task = Task<Unit>(block = { fail("Cancelled owner must not execute a task") })
        queue.enqueue(task)
        owner.cancel()
        queue.cancelAll()

        runCurrent()

        assertEquals(TaskState.Cancelled, task.currentState)
        assertTrue(queue.isIdle)
        // Also cover a scope that was already cancelled at enqueue time.
        val rejected = queue.enqueueWithResult(Task(block = { fail("Closed owner cannot run new work") }))
        assertEquals(EnqueueResult.RejectionCause.CLOSED, (rejected as EnqueueResult.Rejected).cause)
        queue.cancelAll()
        assertTrue(queue.isIdle)
        assertTrue(isActive)
    }

    @Test fun cancellationDoesNotPublishIdleUntilTaskCleanupFinishes() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        val cleanup = CompletableDeferred<Unit>()
        val task = Task<Unit>(block = {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) { cleanup.await() }
            }
        })
        queue.enqueue(task)
        runCurrent()
        try {
            queue.cancelAll()
            runCurrent()
            assertFalse(queue.isIdle)
            assertTrue(queue.state.value is SerialTaskQueue.QueueState.Running)
        } finally {
            cleanup.complete(Unit)
        }
        advanceUntilIdle()
        assertEquals(TaskState.Cancelled, task.currentState)
        assertTrue(queue.isIdle)
        assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
        assertTrue(isActive)
    }

    @Test fun boundedQueueRejectsOverflowAndResumesInOrder() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler), BackpressureConfig.discard(2))
        val completed = mutableListOf<Int>()
        queue.pause()
        val first = Task(block = { completed.add(1) })
        val second = Task(block = { completed.add(2) })
        val overflow = Task(block = { completed.add(3) })
        assertTrue(queue.enqueueWithResult(first) is EnqueueResult.Success)
        assertTrue(queue.enqueueWithResult(second) is EnqueueResult.Success)
        assertTrue(queue.enqueueWithResult(overflow) is EnqueueResult.Rejected)
        assertTrue(overflow.isCancelled)
        queue.resume()
        advanceUntilIdle()
        assertEquals(listOf(1, 2), completed)
        assertTrue(isActive)
    }

    @Test fun cancellingQueueDoesNotCancelItsOwnerScope() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        queue.pause()
        val task = Task(block = { fail("Cancelled queued task must not execute") })
        queue.enqueueWithResult(task)
        queue.cancelAll()
        queue.resume()
        advanceUntilIdle()
        assertTrue(task.isCancelled)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertTrue(isActive)
    }
}
