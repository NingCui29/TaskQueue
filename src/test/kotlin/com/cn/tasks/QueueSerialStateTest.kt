package com.cn.tasks

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueSerialStateTest {
    @Test fun acceptedTaskPublishesWaitingBeforeItsPendingCallbackAndRunsToIdle() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        val release = CompletableDeferred<Unit>()
        var pendingObservedState: SerialTaskQueue.QueueState? = null
        var ran = false
        val task = Task(
            name = "waiting-state-task",
            block = { ran = true; release.await() },
            onStateChange = { state ->
                if (state is TaskState.Pending) pendingObservedState = queue.state.value
            },
        )
        try {
            queue.enqueue(task)

            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            assertEquals(SerialTaskQueue.QueueState.Waiting(1), pendingObservedState)
            assertEquals(1, queue.pendingCount)
            assertEquals(0, queue.runningTaskCount)
            assertFalse(queue.isIdle)
            assertFalse(ran)

            runCurrent()

            assertTrue(ran)
            assertEquals(SerialTaskQueue.QueueState.Running(task.name), queue.state.value)
            assertEquals(1, queue.runningTaskCount)
            release.complete(Unit)
            runCurrent()

            assertEquals(TaskState.Completed(true), task.currentState)
            assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
            assertTrue(queue.isIdle)
        } finally {
            release.complete(Unit)
            queue.shutdown()
            runCurrent()
        }
    }

    @Test fun resumePublishesWaitingBeforeTheWorkerCanStart() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        var ran = false
        val task = Task(block = { ran = true })
        try {
            queue.pause()
            queue.enqueue(task)
            assertEquals(SerialTaskQueue.QueueState.Paused(1), queue.state.value)

            queue.resume()

            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            assertFalse(queue.isPaused)
            assertFalse(ran)
            runCurrent()
            assertTrue(ran)
            assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
            assertTrue(queue.isIdle)
            assertTrue(isActive)
        } finally {
            queue.shutdown()
            runCurrent()
        }
    }

    @Test fun cancellingOrClearingPendingWorkUpdatesWaitingCountAndReleasesIdle() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        val first = Task<Unit>(block = { fail("Cancelled waiting task must not run") })
        val second = Task<Unit>(block = { fail("Cancelled waiting task must not run") })
        val cleared = Task<Unit>(block = { fail("Cleared waiting task must not run") })
        try {
            queue.enqueue(first)
            queue.enqueue(second)
            assertEquals(SerialTaskQueue.QueueState.Waiting(2), queue.state.value)

            assertTrue(queue.cancel(second.id))
            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            assertTrue(queue.cancel(first.id))
            assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
            runCurrent()
            assertTrue(queue.isIdle)

            queue.enqueue(cleared)
            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            queue.clear()
            assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
            runCurrent()
            assertTrue(queue.isIdle)
            assertTrue(cleared.currentState is TaskState.Pending)
            assertEquals(TaskState.Cancelled, first.currentState)
            assertEquals(TaskState.Cancelled, second.currentState)
        } finally {
            queue.shutdown()
            runCurrent()
        }
    }

    @Test fun shutdownFromWaitingCollectorCannotRestoreWaitingOrStartTheTask() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        var closedFromCollector = false
        val collector = backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            queue.state.collect { state ->
                if (state is SerialTaskQueue.QueueState.Waiting && !closedFromCollector) {
                    closedFromCollector = true
                    queue.shutdown()
                }
            }
        }
        val task = Task<Unit>(block = { fail("Shutdown during waiting publication must prevent execution") })
        try {
            queue.enqueue(task)
            runCurrent()

            assertTrue(closedFromCollector)
            assertEquals(TaskState.Cancelled, task.currentState)
            assertEquals(SerialTaskQueue.QueueState.Shutdown, queue.state.value)
            assertTrue(queue.isTerminated)
            assertTrue(isActive)
        } finally {
            collector.cancel()
            queue.shutdown()
            runCurrent()
        }
    }

    @Test fun waitingCollectorCanReplaceCancelledWorkWithoutRestoringTheOriginalTask() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        var replaced = false
        val replacement = Task(block = { Unit })
        val collector = backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            queue.state.collect { state ->
                if (state is SerialTaskQueue.QueueState.Waiting && !replaced) {
                    replaced = true
                    queue.cancelAll()
                    queue.enqueue(replacement)
                }
            }
        }
        val original = Task<Unit>(block = { fail("Replaced waiting work must never execute") })
        try {
            queue.enqueue(original)

            assertTrue(replaced)
            assertEquals(TaskState.Cancelled, original.currentState)
            assertEquals(SerialTaskQueue.QueueState.Waiting(1), queue.state.value)
            runCurrent()

            assertEquals(TaskState.Completed(true), replacement.currentState)
            assertEquals(TaskState.Cancelled, original.currentState)
            assertEquals(SerialTaskQueue.QueueState.Idle, queue.state.value)
            assertTrue(queue.isIdle)
            assertTrue(isActive)
        } finally {
            collector.cancel()
            queue.shutdown()
            runCurrent()
        }
    }
}
