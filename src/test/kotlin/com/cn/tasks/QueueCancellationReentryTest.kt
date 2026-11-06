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
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueCancellationReentryTest {
    @Test fun serialCancelledCallbackMayCancelAllAgainWithoutRepeatingItsTerminalNotification() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseCleanup = CompletableDeferred<Unit>()
        var cancelledNotifications = 0
        var completions = 0
        var exits = 0
        val task = Task<Unit>(
            block = {
                try {
                    awaitCancellation()
                } finally {
                    withContext(NonCancellable) { releaseCleanup.await() }
                    exits++
                }
            },
            onStateChange = { state ->
                if (state is TaskState.Cancelled) {
                    cancelledNotifications++
                    if (cancelledNotifications == 1) queue.cancelAll()
                }
            },
            onComplete = { completions++ },
        )
        try {
            queue.enqueue(task)
            runCurrent()
            assertTrue(task.currentState is TaskState.Running)

            queue.cancelAll()
            runCurrent()

            assertEquals(1, cancelledNotifications)
            assertEquals(1, queue.runningTaskCount)
            assertFalse(queue.isIdle)
            assertEquals(0, completions)
            assertTrue(owner.isActive)

            releaseCleanup.complete(Unit)
            runCurrent()

            assertEquals(TaskState.Cancelled, task.currentState)
            assertEquals(1, cancelledNotifications)
            assertEquals(1, completions)
            assertEquals(1, exits)
            assertEquals(0, queue.runningTaskCount)
            assertTrue(queue.isIdle)
            val next = queue.enqueue(Task(block = { Unit }))
            runCurrent()
            assertEquals(TaskState.Completed(true), next.currentState)
        } finally {
            releaseCleanup.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    @Test fun concurrentCancelledCallbackMayCancelItsHandoffTaskOrAllTasksAgain() = runTest {
        for (cancelAllFromCallback in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = ConcurrentTaskQueue(owner, 1, dispatcher)
            var armed = false
            var initiated = false
            var cancelledNotifications = 0
            lateinit var first: Task<Unit>
            first = Task(
                block = { fail("Cancelled handoff task must not execute") },
                onStateChange = { state ->
                    if (state is TaskState.Cancelled) {
                        cancelledNotifications++
                        if (cancelledNotifications == 1) {
                            if (cancelAllFromCallback) queue.cancelAll() else assertTrue(queue.cancel(first.id))
                        }
                    }
                },
            )
            val second = Task(
                block = { Unit },
                onStateChange = { state ->
                    if (armed && !initiated && state is TaskState.Pending && state.queueSize == 1) {
                        initiated = true
                        assertTrue(queue.cancel(first.id))
                    }
                },
            )
            try {
                queue.enqueue(first)
                queue.enqueue(second)
                armed = true
                runCurrent()

                assertTrue(initiated)
                assertEquals(1, cancelledNotifications)
                assertEquals(TaskState.Cancelled, first.currentState)
                assertEquals(
                    if (cancelAllFromCallback) TaskState.Cancelled else TaskState.Completed(true),
                    second.currentState,
                )
                assertEquals(0, queue.runningTaskCount)
                assertEquals(0, queue.pendingTaskCount)
                assertTrue(queue.isIdle)
                assertTrue(owner.isActive)

                val next = queue.enqueue(Task(block = { Unit }))
                runCurrent()
                queue.awaitCompletion()
                assertEquals(TaskState.Completed(true), next.currentState)
                assertTrue(queue.isIdle)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }
}
