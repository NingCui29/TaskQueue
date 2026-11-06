package com.cn.tasks

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.time.Duration.Companion.milliseconds
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class TaskProgressLifecycleTest {
    @Test fun aSavedReporterCannotPublishDuringSuccessOrAfterCompletion() = runTest {
        lateinit var reporter: ProgressReporter
        val states = mutableListOf<TaskState>()
        val task = Task(
            block = { reporter = it; it.report(0.5f); 42 },
            onSuccess = { reporter.report(0.8f, "too late") },
            onStateChange = { states += it },
        )

        assertEquals(42, task.execute().getOrThrow())
        reporter.report(1f, "also late")

        assertEquals(
            listOf(TaskState.Running(), TaskState.Running(0.5f), TaskState.Completed(true)),
            states,
        )
        assertEquals(TaskState.Completed(true), task.currentState)
    }

    @Test fun cancellationCleanupAndRetainedCallbacksCannotReviveProgress() = runTest {
        lateinit var reporter: ProgressReporter
        val states = mutableListOf<TaskState>()
        val task = Task<Unit>(
            block = {
                reporter = it
                try { awaitCancellation() } finally { reporter.report(0.7f, "cleanup") }
            },
            onStateChange = { states += it },
        )
        val execution = async { task.execute() }
        runCurrent()

        execution.cancel()
        runCurrent()
        reporter.report(1f, "late")

        assertEquals(listOf(TaskState.Running(), TaskState.Cancelled), states)
        assertEquals(TaskState.Cancelled, task.currentState)
    }

    @Test fun configuredTimeoutClosesItsReporterBeforeErrorCallbacks() = runTest {
        lateinit var reporter: ProgressReporter
        val states = mutableListOf<TaskState>()
        val task = Task<Unit>(
            timeout = 10.milliseconds,
            block = { reporter = it; awaitCancellation() },
            onTimeout = { reporter.report(0.8f, "timeout callback") },
            onError = { reporter.report(0.9f, "error callback") },
            onStateChange = { states += it },
        )

        val result = task.execute()
        reporter.report(1f, "late")

        assertTrue(result.exceptionOrNull() is TaskTimeoutException)
        assertEquals(2, states.size)
        assertEquals(TaskState.Running(), states.first())
        assertTrue(states.last() is TaskState.Completed)
        assertEquals(states.last(), task.currentState)
    }

    @Test fun nanIsIgnoredAndFiniteRangeClampingRemainsAvailable() = runTest {
        val progress = mutableListOf<Float>()
        val task = Task(
            block = {
                it.report(0.5f)
                it.report(Float.NaN)
                it.report(-2f)
                it.report(3f)
            },
            onStateChange = { if (it is TaskState.Running) progress += it.progress },
        )

        task.execute().getOrThrow()

        assertEquals(listOf(0f, 0.5f, 0f, 1f), progress)
    }
}
