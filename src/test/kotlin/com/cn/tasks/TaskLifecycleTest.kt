package com.cn.tasks

import java.io.IOException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class TaskLifecycleTest {
    @Test fun downstreamTimeoutDoesNotInvokeTheOuterTasksOwnTimeoutCallback() = runTest {
        for (outerTimeout in listOf(Duration.INFINITE, 100.milliseconds)) {
            val downstream = Task<Unit>(
                name = "downstream",
                timeout = 10.milliseconds,
                block = { awaitCancellation() },
            )
            val callbacks = mutableListOf<String>()
            var deliveredError: Throwable? = null
            val outer = Task<Unit>(
                name = "outer",
                timeout = outerTimeout,
                block = { downstream.execute().getOrThrow() },
                onTimeout = { callbacks += "timeout" },
                onError = { deliveredError = it; callbacks += "error" },
                onComplete = { callbacks += "complete" },
            )

            val result = outer.execute()
            val error = result.exceptionOrNull()

            assertTrue(error is TaskTimeoutException)
            assertEquals("downstream", (error as TaskTimeoutException).taskName)
            assertSame(error, deliveredError)
            assertTrue(downstream.isTimeout)
            assertFalse(outer.isTimeout)
            assertFalse(outer.isCancelled)
            assertEquals(TaskState.Completed(false, error), outer.currentState)
            assertEquals(listOf("error", "complete"), callbacks)
        }
    }

    @Test fun aBusinessTimeoutExceptionIsDeliveredWithoutClaimingTheConfiguredTimeout() = runTest {
        val downstreamFailure = TaskTimeoutException("downstream", 10.milliseconds)
        var timeouts = 0
        var errors = 0
        val task = Task<Unit>(
            block = { throw downstreamFailure },
            onTimeout = { timeouts++ },
            onError = { assertSame(downstreamFailure, it); errors++ },
        )

        val result = task.execute()

        assertSame(downstreamFailure, result.exceptionOrNull())
        assertFalse(task.isTimeout)
        assertEquals(0, timeouts)
        assertEquals(1, errors)
        assertEquals(TaskState.Completed(false, downstreamFailure), task.currentState)
    }

    @Test fun timeoutOwnershipBelongsToEachExecutionRatherThanTheStoredTimeoutFlag() = runTest {
        val downstreamFailure = TaskTimeoutException("downstream", 10.milliseconds)
        var executions = 0
        var timeouts = 0
        val task = Task<Unit>(
            timeout = 10.milliseconds,
            block = {
                executions++
                if (executions == 1) awaitCancellation() else throw downstreamFailure
            },
            onTimeout = { timeouts++ },
        )

        assertTrue(task.execute().exceptionOrNull() is TaskTimeoutException)
        assertTrue(task.isTimeout)
        assertEquals(1, timeouts)

        val next = task.execute()

        assertSame(downstreamFailure, next.exceptionOrNull())
        assertEquals(1, timeouts)
        assertEquals(TaskState.Completed(false, downstreamFailure), task.currentState)
    }

    @Test fun theConfiguredTimeoutRemainsATaskFailure() = runTest {
        val callbacks = mutableListOf<String>()
        val task = Task<Unit>(
            timeout = 10.milliseconds,
            block = { awaitCancellation() },
            onTimeout = { callbacks += "timeout" },
            onError = { callbacks += "error" },
            onComplete = { callbacks += "complete" },
        )

        val result = task.execute()

        assertTrue(result.exceptionOrNull() is TaskTimeoutException)
        assertTrue(task.isTimeout)
        assertFalse(task.isCancelled)
        assertEquals(listOf("timeout", "error", "complete"), callbacks)
    }

    @Test fun anOuterTimeoutIsNotReportedAsTheTasksOwnTimeout() = runTest {
        var errors = 0
        var completions = 0
        val task = Task<Unit>(
            block = { awaitCancellation() },
            onError = { errors++ },
            onComplete = { completions++ },
        )

        val failure = runCatching { withTimeout(10) { task.execute() } }.exceptionOrNull()

        assertTrue(failure is TimeoutCancellationException)
        assertFalse(task.isTimeout)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(0, errors)
        assertEquals(1, completions)
    }

    @Test fun aNestedTimeoutIsNotReportedAsTheTasksConfiguredTimeout() = runTest {
        val task = Task<Unit>(
            timeout = 100.milliseconds,
            block = { withTimeout(10) { awaitCancellation() } },
        )

        val failure = runCatching { task.execute() }.exceptionOrNull()

        assertTrue(failure is TimeoutCancellationException)
        assertFalse(task.isTimeout)
        assertEquals(TaskState.Cancelled, task.currentState)
    }

    @Test fun aValueReturnedAfterCancellationDoesNotReachSuccess() = runTest {
        var successes = 0
        var completions = 0
        val task = Task(
            block = { currentCoroutineContext().cancel(); 42 },
            onSuccess = { successes++ },
            onComplete = { completions++ },
        )
        val execution = async { task.execute() }
        runCurrent()

        assertTrue(execution.isCancelled)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(0, successes)
        assertEquals(1, completions)
    }

    @Test fun aFailureAfterCancellationDoesNotReachError() = runTest {
        var errors = 0
        val task = Task<Unit>(
            block = { currentCoroutineContext().cancel(); throw IOException("late failure") },
            onError = { errors++ },
        )
        val execution = async { task.execute() }
        runCurrent()

        assertTrue(execution.isCancelled)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(0, errors)
    }

    @Test fun cancellationFromSuccessStillCompletesExactlyOnce() = runTest {
        val cancellation = CancellationException("callback cancelled")
        var completions = 0
        val task = Task(
            block = { 42 },
            onSuccess = { throw cancellation },
            onComplete = { completions++ },
        )

        val failure = runCatching { task.execute() }.exceptionOrNull()

        assertSame(cancellation, failure)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(1, completions)
    }

    @Test fun aThrowingCancelledStateCallbackDoesNotSkipCompletion() = runTest {
        val cancellation = CancellationException("state callback cancelled")
        var completions = 0
        val task = Task<Unit>(
            block = { error("must not run") },
            onStateChange = { if (it is TaskState.Cancelled) throw cancellation },
            onComplete = { completions++ },
        )
        task.cancel()

        val failure = runCatching { task.execute() }.exceptionOrNull()

        assertSame(cancellation, failure)
        assertEquals(1, completions)
    }
}
