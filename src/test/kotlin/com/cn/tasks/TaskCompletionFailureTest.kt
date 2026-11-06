package com.cn.tasks

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class TaskCompletionFailureTest {
    @Test fun fatalBodyOrRunningCallbackFailureSurvivesCompletionCancellation() = runTest {
        for (fromCallback in listOf(false, true)) {
            val primaryFailures = listOf(AssertionError("execution failed"), object : Throwable("fatal execution") {})
            for (fatal in primaryFailures) {
                val completion = CancellationException("completion cancelled")
                var completions = 0
                val task = failingTask(fromCallback, fatal) { completions++; throw completion }

                val propagated = runCatching { task.execute() }.exceptionOrNull()

                assertSame(fatal, propagated)
                assertEquals(listOf(completion), fatal.suppressed.toList())
                assertEquals(1, completions)
                assertTrue(isActive)
            }
        }
    }

    @Test fun fatalCompletionFailureTakesPriorityOverExecutionCancellation() = runTest {
        val cancellation = CancellationException("execution cancelled")
        val fatal = LinkageError("completion failed")
        var completions = 0
        val task = Task<Unit>(
            block = { throw cancellation },
            onComplete = { completions++; throw fatal },
        )

        val propagated = runCatching { task.execute() }.exceptionOrNull()

        assertSame(fatal, propagated)
        assertEquals(listOf(cancellation), fatal.suppressed.toList())
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(1, completions)
        assertTrue(isActive)
    }

    @Test fun equalPriorityFailuresKeepTheOriginalAndSuppressTheCompletionFailure() = runTest {
        val pairs = listOf(
            AssertionError("execution failed") to LinkageError("completion failed"),
            CancellationException("execution cancelled") to CancellationException("completion cancelled"),
        )
        for ((primary, completion) in pairs) {
            var completions = 0
            val task = Task<Unit>(
                block = { throw primary },
                onComplete = { completions++; throw completion },
            )

            val propagated = runCatching { task.execute() }.exceptionOrNull()

            assertSame(primary, propagated)
            assertEquals(listOf(completion), primary.suppressed.toList())
            assertEquals(1, completions)
        }
    }

    @Test fun rethrowingTheSameFailureNeverAttemptsSelfSuppression() = runTest {
        for (failure in listOf(AssertionError("same fatal"), CancellationException("same cancellation"))) {
            var completions = 0
            val task = Task<Unit>(
                block = { throw failure },
                onComplete = { completions++; throw failure },
            )

            val propagated = runCatching { task.execute() }.exceptionOrNull()

            assertSame(failure, propagated)
            assertTrue(failure.suppressed.isEmpty())
            assertEquals(1, completions)
        }
    }

    @Test fun cancelledStateCallbackRemainsTheFirstEscapingFailure() = runTest {
        val stateFailure = CancellationException("cancelled state callback")
        val completionFailure = CancellationException("completion callback")
        var completions = 0
        val task = Task<Unit>(
            block = { fail("Already cancelled task must not execute") },
            onStateChange = { if (it is TaskState.Cancelled) throw stateFailure },
            onComplete = { completions++; throw completionFailure },
        )
        task.cancel()

        val propagated = runCatching { task.execute() }.exceptionOrNull()

        assertSame(stateFailure, propagated)
        assertEquals(listOf(completionFailure), stateFailure.suppressed.toList())
        assertEquals(TaskState.Cancelled, task.currentState)
        assertEquals(1, completions)
    }

    @Test fun ordinaryCompletionExceptionStillDoesNotReplaceTheBusinessResult() = runTest {
        var completions = 0
        val task = Task(
            block = { 42 },
            onComplete = { completions++; throw IOException("completion observer failed") },
        )

        val result = task.execute()

        assertEquals(42, result.getOrThrow())
        assertEquals(TaskState.Completed(true), task.currentState)
        assertEquals(1, completions)
    }

    @Test fun callerRunsPropagatesTheFatalFailureAndReleasesItsOwnership() {
        for (concurrent in listOf(false, true)) for (fromCallback in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.callerRuns(1))
            val fatal = AssertionError("execution failed")
            val completion = CancellationException("completion cancelled")
            var completions = 0
            val failed = failingTask(fromCallback, fatal) { completions++; throw completion }
            val filler = Task(block = { Unit })
            try {
                queue.enqueue(filler)

                val propagated = runCatching { queue.enqueue(failed) }.exceptionOrNull()

                assertSame(fatal, propagated)
                assertTrue(fatal.suppressed.any { it === completion })
                assertEquals(1, completions)
                assertEquals(0, queue.running())
                assertTrue(owner.isActive)
                val replacement = Task(id = failed.id, block = { Unit })
                assertTrue((queue.enqueue(replacement) as EnqueueResult.CallerExecuted).success)
                dispatcher.runAll()
                assertEquals(TaskState.Completed(true), filler.currentState)
                assertEquals(TaskState.Completed(true), replacement.currentState)
                assertTrue(queue.isIdle())
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun ordinaryWorkersReportTheFatalFailureAndContinueAfterCleanup() = runTest {
        for (concurrent in listOf(false, true)) {
            val failures = mutableListOf<Throwable>()
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, error ->
                failures += error
            })
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.unbounded())
            val fatal = AssertionError("execution failed")
            val completion = CancellationException("completion cancelled")
            var completions = 0
            val failed = failingTask(false, fatal) { completions++; throw completion }
            val next = Task(block = { Unit })
            try {
                queue.enqueue(failed)
                queue.enqueue(next)
                runCurrent()

                assertEquals(listOf(fatal), failures)
                assertTrue(fatal.suppressed.any { it === completion })
                assertEquals(1, completions)
                assertEquals(TaskState.Cancelled, failed.currentState)
                assertEquals(TaskState.Completed(true), next.currentState)
                assertEquals(0, queue.running())
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    private fun failingTask(fromCallback: Boolean, failure: Throwable, completion: () -> Unit) = Task<Unit>(
        block = { if (!fromCallback) throw failure },
        onStateChange = { if (fromCallback && it is TaskState.Running) throw failure },
        onComplete = completion,
    )

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig,
    ): QueueAccess = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueAccess(queue::enqueueWithResult, { queue.runningTaskCount }, { queue.isIdle }, queue::shutdown)
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config)
        QueueAccess(queue::enqueueWithResult, { queue.runningTaskCount }, { queue.isIdle }, queue::shutdown)
    }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> EnqueueResult,
        val running: () -> Int,
        val isIdle: () -> Boolean,
        val shutdown: () -> Unit,
    )

    private class PausedDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { work.add(block) }
        fun runAll() { while (true) (work.poll() ?: return).run() }
    }
}
