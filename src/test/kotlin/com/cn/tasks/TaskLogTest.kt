package com.cn.tasks

import com.cn.tasks.log.TaskLog
import java.io.IOException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Before
import org.junit.Test

class TaskLogTest {
    private var originalEnabled = false
    private var originalPriority = TaskLog.PRIORITY_DEBUG
    private lateinit var originalPrinter: TaskLog.Printer

    @Before fun saveLogConfiguration() {
        originalEnabled = TaskLog.enabled
        originalPriority = TaskLog.minPriority
        originalPrinter = TaskLog.printer
        TaskLog.enabled = true
        TaskLog.minPriority = TaskLog.PRIORITY_DEBUG
    }

    @After fun restoreLogConfiguration() {
        TaskLog.enabled = originalEnabled
        TaskLog.minPriority = originalPriority
        TaskLog.printer = originalPrinter
    }

    @Test fun ordinaryPrinterFailuresDoNotInvalidateSuccessfulTasks() = runTest {
        var logAttempts = 0
        var completions = 0
        TaskLog.printer = TaskLog.Printer { _, _, _, _ ->
            logAttempts++
            throw IOException("log sink unavailable")
        }
        val task = Task(
            block = { 42 },
            onSuccess = { throw IOException("success observer failed") },
            onComplete = { completions++; throw IOException("completion observer failed") },
        )

        val result = task.execute()

        assertEquals(42, result.getOrThrow())
        assertEquals(TaskState.Completed(true), task.currentState)
        assertEquals(1, completions)
        assertEquals(2, logAttempts)
    }

    @Test fun ordinaryPrinterFailurePreservesTheOriginalBusinessFailure() = runTest {
        val businessFailure = object : Exception("business failed") {}
        var deliveredFailure: Throwable? = null
        var completions = 0
        TaskLog.printer = TaskLog.Printer { _, _, _, _ -> throw IOException("log sink unavailable") }
        val task = Task<Unit>(
            block = { throw businessFailure },
            onError = { deliveredFailure = it; throw IOException("error observer failed") },
            onComplete = { completions++ },
        )

        val result = task.execute()

        assertSame(businessFailure, result.exceptionOrNull())
        assertSame(businessFailure, deliveredFailure)
        assertEquals(TaskState.Completed(false, businessFailure), task.currentState)
        assertEquals(1, completions)
    }

    @Test fun disabledLoggingDoesNotCallThePrinterOrConstructLazyMessages() {
        var logAttempts = 0
        var messageConstructed = false
        TaskLog.enabled = false
        TaskLog.printer = TaskLog.Printer { _, _, _, _ -> logAttempts++ }

        TaskLog.d("debug")
        TaskLog.d { messageConstructed = true; "lazy debug" }
        TaskLog.w("warning")
        TaskLog.e("error")

        assertEquals(0, logAttempts)
        assertFalse(messageConstructed)
    }

    @Test fun printerCancellationStillCancelsTheTaskAndPropagates() = runTest {
        val cancellation = CancellationException("logging cancelled")
        var completions = 0
        TaskLog.printer = TaskLog.Printer { _, _, _, _ -> throw cancellation }
        val task = Task(
            block = { 42 },
            onSuccess = { throw IOException("success observer failed") },
            onComplete = { completions++ },
        )

        val propagated = runCatching { task.execute() }.exceptionOrNull()

        assertSame(cancellation, propagated)
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(1, completions)
    }

    @Test fun fatalPrinterErrorsStillPropagateAfterTaskCompletionNotification() = runTest {
        val fatal = AssertionError("log sink failed fatally")
        var completions = 0
        TaskLog.printer = TaskLog.Printer { _, _, _, _ -> throw fatal }
        val task = Task(
            block = { 42 },
            onSuccess = { throw IOException("success observer failed") },
            onComplete = { completions++ },
        )

        val propagated = runCatching { task.execute() }.exceptionOrNull()

        assertSame(fatal, propagated)
        assertEquals(1, completions)
    }
}
