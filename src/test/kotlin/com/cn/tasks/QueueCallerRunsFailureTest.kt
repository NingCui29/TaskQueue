package com.cn.tasks

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import org.junit.Assert.*
import org.junit.Test

class QueueCallerRunsFailureTest {
    @Test fun fatalFailuresEscapeTheCallerAfterOwnedExecutionHasBeenCleanedUp() {
        for (concurrent in listOf(false, true)) {
            for (site in FailureSite.entries) {
                val failures = listOf(AssertionError("assertion failure"), LinkageError("linkage failure"),
                    object : Throwable("non-exception failure") {})
                for (failure in failures) {
                    val dispatcher = PausedDispatcher()
                    val owner = CoroutineScope(SupervisorJob() + dispatcher)
                    val queue = queue(owner, dispatcher, concurrent)
                    var completions = 0
                    val task = Task<Unit>(
                        block = { if (site == FailureSite.BODY) throw failure },
                        onStateChange = {
                            if (site == FailureSite.RUNNING_CALLBACK && it is TaskState.Running) throw failure
                        },
                        onComplete = {
                            completions++
                            if (site == FailureSite.COMPLETION_CALLBACK) throw failure
                        },
                    )
                    try {
                        val filler = Task(block = { Unit })
                        queue.enqueue(filler)

                        val propagated = runCatching { queue.enqueue(task) }.exceptionOrNull()

                        assertSame("concurrent=$concurrent site=$site", failure, propagated)
                        assertEquals(1, completions)
                        assertEquals(0, queue.running())
                        assertEquals(1, queue.pending())
                        assertTrue(owner.isActive)

                        val replacement = Task(id = task.id, block = { Unit })
                        val replacementResult = queue.enqueue(replacement) as EnqueueResult.CallerExecuted
                        assertTrue(replacementResult.success)
                        assertEquals(TaskState.Completed(true), replacement.currentState)
                        assertEquals(0, queue.running())

                        dispatcher.runAll()
                        assertEquals(TaskState.Completed(true), filler.currentState)
                        assertTrue(queue.isIdle())
                        assertTrue(owner.isActive)
                    } finally {
                        queue.shutdown()
                        owner.cancel()
                        dispatcher.runAll()
                    }
                }
            }
        }
    }

    @Test fun ordinaryBusinessFailuresRemainCallerExecutedResults() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            val failure = IOException("business failure")
            var delivered: Throwable? = null
            var completions = 0
            val task = Task<Unit>(
                block = { throw failure },
                onError = { delivered = it },
                onComplete = { completions++ },
            )
            try {
                queue.enqueue(Task(block = { Unit }))

                val result = queue.enqueue(task) as EnqueueResult.CallerExecuted

                assertFalse(result.success)
                assertSame(failure, result.error)
                assertSame(failure, delivered)
                assertEquals(TaskState.Completed(false, failure), task.currentState)
                assertEquals(1, completions)
                assertEquals(0, queue.running())
                assertTrue(owner.isActive)
                dispatcher.runAll()
                assertTrue(queue.isIdle())
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    @Test fun cancellationRetainsItsExistingCallerExecutedFailureAndCleansUpOnce() {
        for (concurrent in listOf(false, true)) {
            val dispatcher = PausedDispatcher()
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            var errors = 0
            var completions = 0
            val task = Task<Unit>(
                block = { throw CancellationException("caller task cancelled") },
                onError = { errors++ },
                onComplete = { completions++ },
            )
            try {
                queue.enqueue(Task(block = { Unit }))

                val result = queue.enqueue(task) as EnqueueResult.CallerExecuted

                assertFalse(result.success)
                assertTrue(result.error is CancellationException)
                assertEquals(TaskState.Cancelled, task.currentState)
                assertEquals(0, errors)
                assertEquals(1, completions)
                assertEquals(0, queue.running())
                assertTrue(owner.isActive)
                val next = queue.enqueue(Task(block = { Unit })) as EnqueueResult.CallerExecuted
                assertTrue(next.success)
                dispatcher.runAll()
                assertTrue(queue.isIdle())
            } finally {
                queue.shutdown()
                owner.cancel()
                dispatcher.runAll()
            }
        }
    }

    private enum class FailureSite { BODY, RUNNING_CALLBACK, COMPLETION_CALLBACK }

    private fun queue(owner: CoroutineScope, dispatcher: CoroutineDispatcher, concurrent: Boolean): QueueAccess =
        if (concurrent) {
            val queue = ConcurrentTaskQueue(owner, 1, dispatcher, BackpressureConfig.callerRuns(1))
            QueueAccess(queue::enqueueWithResult, { queue.runningTaskCount }, { queue.pendingTaskCount },
                { queue.isIdle }, queue::shutdown)
        } else {
            val queue = SerialTaskQueue(owner, dispatcher, BackpressureConfig.callerRuns(1))
            QueueAccess(queue::enqueueWithResult, { queue.runningTaskCount }, { queue.pendingCount },
                { queue.isIdle }, queue::shutdown)
        }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> EnqueueResult,
        val running: () -> Int,
        val pending: () -> Int,
        val isIdle: () -> Boolean,
        val shutdown: () -> Unit,
    )

    private class PausedDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            work.add(block)
        }

        fun runAll() {
            while (true) (work.poll() ?: return).run()
        }
    }
}
