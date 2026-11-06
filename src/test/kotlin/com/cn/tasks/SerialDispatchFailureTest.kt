package com.cn.tasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SerialDispatchFailureTest {
    @Test fun fatalPendingNotificationIsReportedWithoutStartingTheReservedTasksBusinessBody() = runTest {
        verifyFatalHandoff(cleanupFailure = null)
    }

    @Test fun reservedTaskCancellationFailuresDoNotReplaceTheOriginalFatalNotification() = runTest {
        for (cleanupFailure in listOf(
            CancellationException("cancelled observer cancellation"),
            LinkageError("cancelled observer fatal failure"),
            object : Throwable("cancelled observer non-exception failure") {},
        )) {
            verifyFatalHandoff(cleanupFailure)
        }
    }

    @Test fun shutdownFromTheCancellationCallbackStillReportsTheFatalFailureAndTerminates() = runTest {
        val failures = mutableListOf<Throwable>()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, failure ->
            failures += failure
        })
        val queue = SerialTaskQueue(owner, dispatcher)
        val fatal = AssertionError("fatal pending notification")
        val cancellation = CancellationException("cancelled observer after shutdown")
        var armed = false
        var cancellations = 0
        val reserved = Task<Unit>(
            block = { fail("Failed reservation must not execute") },
            onStateChange = { state ->
                if (state is TaskState.Cancelled) {
                    cancellations++
                    queue.shutdown()
                    throw cancellation
                }
            },
        )
        val remaining = Task<Unit>(
            block = { fail("Shutdown must cancel pending work") },
            onStateChange = { state ->
                if (armed && state is TaskState.Pending && state.queueSize == 1) throw fatal
            },
        )
        try {
            queue.enqueue(reserved)
            queue.enqueue(remaining)
            armed = true
            runCurrent()

            assertEquals(listOf(fatal), failures)
            assertEquals(listOf(cancellation), fatal.suppressed.toList())
            assertEquals(TaskState.Cancelled, reserved.currentState)
            assertEquals(TaskState.Cancelled, remaining.currentState)
            assertEquals(1, cancellations)
            assertEquals(0, queue.runningTaskCount)
            assertEquals(0, queue.pendingCount)
            assertEquals(SerialTaskQueue.QueueState.Shutdown, queue.state.value)
            assertTrue(queue.isTerminated)
            assertTrue(owner.isActive)
        } finally {
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    private fun TestScope.verifyFatalHandoff(cleanupFailure: Throwable?) {
        val failures = mutableListOf<Throwable>()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, failure ->
            failures += failure
        })
        val queue = SerialTaskQueue(owner, dispatcher)
        val releaseBusiness = CompletableDeferred<Unit>()
        val fatal = AssertionError("fatal pending notification")
        var armed = false
        var notifiedFatally = false
        var businessStarted = false
        var cancellations = 0
        var completions = 0
        val reserved = Task<Unit>(
            block = {
                businessStarted = true
                releaseBusiness.await()
            },
            onStateChange = { state ->
                if (state is TaskState.Cancelled) {
                    cancellations++
                    cleanupFailure?.let { throw it }
                }
            },
            onComplete = { completions++ },
        )
        val remaining = Task(block = { Unit }, onStateChange = { state ->
            if (armed && !notifiedFatally && state is TaskState.Pending && state.queueSize == 1) {
                notifiedFatally = true
                throw fatal
            }
        })
        try {
            queue.enqueue(reserved)
            queue.enqueue(remaining)
            armed = true

            runCurrent()

            assertTrue(notifiedFatally)
            assertFalse("Reporting the fatal failure must not wait for another business body", releaseBusiness.isCompleted)
            assertEquals(1, failures.size)
            assertSame(fatal, failures.single())
            if (cleanupFailure == null) {
                assertEquals(0, fatal.suppressed.size)
            } else {
                assertEquals(1, fatal.suppressed.size)
                assertSame(cleanupFailure, fatal.suppressed.single())
            }
            assertFalse("A failed dispatch reservation must never enter execute", businessStarted)
            assertTrue(reserved.isCancelled)
            assertEquals(TaskState.Cancelled, reserved.currentState)
            assertEquals(1, cancellations)
            assertEquals(0, completions)
            assertEquals(TaskState.Completed(true), remaining.currentState)
            assertEquals(0, queue.pendingCount)
            assertEquals(0, queue.runningTaskCount)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)

            val replacement = queue.enqueue(Task(id = reserved.id, block = { Unit }))
            runCurrent()

            assertEquals(TaskState.Completed(true), replacement.currentState)
            assertEquals(1, failures.size)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            releaseBusiness.complete(Unit)
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }
}
