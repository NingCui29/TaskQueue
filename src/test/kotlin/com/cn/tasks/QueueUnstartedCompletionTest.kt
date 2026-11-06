package com.cn.tasks

import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

/** Accepted work has one exit notification even when cancellation prevents its body from starting. */
@OptIn(ExperimentalCoroutinesApi::class)
class QueueUnstartedCompletionTest {
    @Test fun pendingCancellationAndDiscardCompleteAcceptedTasksExactlyOnce() = runTest {
        for (concurrent in listOf(false, true)) {
            for (action in PendingAction.entries) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val queue = queue(owner, dispatcher, concurrent, action.config())
                val events = mutableListOf<String>()
                var completions = 0
                var replacementRuns = 0
                val victim = Task<Unit>(
                    block = { fail("Cancelled pending work must not start") },
                    onStateChange = { if (it is TaskState.Cancelled) events += "cancelled" },
                    onComplete = { completions++; events += "complete" },
                )
                val replacement = Task<Unit>(block = { replacementRuns++ })
                try {
                    queue.enqueue(victim)
                    assertEquals(0, completions)

                    action.apply(queue, victim, replacement)

                    assertEquals("concurrent=$concurrent action=$action", listOf("cancelled", "complete"), events)
                    assertEquals(TaskState.Cancelled, victim.currentState)
                    assertTrue(victim.isCancelled)
                    assertEquals(1, completions)
                    assertFalse(queue.cancel(victim.id))
                    runCurrent()
                    assertEquals(if (action == PendingAction.DISCARD_OLDEST) 1 else 0, replacementRuns)
                    assertTrue(queue.isIdle())

                    queue.cancelAll()
                    queue.shutdown()
                    runCurrent()
                    assertEquals(1, completions)
                    assertTrue(owner.isActive)
                } finally {
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }

    @Test fun cancelledStateFailuresStillCompleteAndPreserveFailurePriority() = runTest {
        for (concurrent in listOf(false, true)) {
            for (action in PendingAction.entries) {
                for (case in failureCases()) {
                    val dispatcher = StandardTestDispatcher(testScheduler)
                    val owner = CoroutineScope(SupervisorJob() + dispatcher)
                    val queue = queue(owner, dispatcher, concurrent, action.config())
                    var cancellations = 0
                    var completions = 0
                    var replacementRuns = 0
                    val victim = Task<Unit>(
                        block = { fail("Cancelled pending work must not start") },
                        onStateChange = {
                            if (it is TaskState.Cancelled) {
                                cancellations++
                                throw case.stateFailure
                            }
                        },
                        onComplete = {
                            completions++
                            case.completionFailure?.let { throw it }
                        },
                    )
                    val replacement = Task<Unit>(block = { replacementRuns++ })
                    try {
                        queue.enqueue(victim)

                        val reported = runCatching { action.apply(queue, victim, replacement) }.exceptionOrNull()

                        assertSame("concurrent=$concurrent action=$action", case.expected, reported)
                        assertEquals(case.suppressed, case.expected.suppressed.toList())
                        assertEquals(TaskState.Cancelled, victim.currentState)
                        assertEquals(1, cancellations)
                        assertEquals(1, completions)
                        assertFalse(queue.cancel(victim.id))
                        runCurrent()
                        assertEquals(if (action == PendingAction.DISCARD_OLDEST) 1 else 0, replacementRuns)
                        assertTrue(queue.isIdle())
                        assertTrue(owner.isActive)
                    } finally {
                        queue.shutdown()
                        owner.cancel()
                        runCurrent()
                    }
                    assertEquals(1, completions)
                }
            }
        }
    }

    @Test fun cancelAllCompletesEveryPendingTaskAfterAnEarlierCompletionFails() = runTest {
        for (concurrent in listOf(false, true)) {
            for (shutdown in listOf(false, true)) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val queue = queue(owner, dispatcher, concurrent)
                val cancellation = CancellationException("first completion")
                val fatal = AssertionError("second completion")
                val completed = mutableListOf<String>()
                val first = Task<Unit>(
                    block = { fail("Cancelled work must not start") },
                    onComplete = { completed += "first"; throw cancellation },
                )
                val second = Task<Unit>(
                    block = { fail("Cancelled work must not start") },
                    onComplete = { completed += "second"; throw fatal },
                )
                try {
                    queue.enqueue(first)
                    queue.enqueue(second)

                    val reported = runCatching {
                        if (shutdown) queue.shutdown() else queue.cancelAll()
                    }.exceptionOrNull()

                    assertSame(fatal, reported)
                    assertEquals(listOf(cancellation), fatal.suppressed.toList())
                    assertEquals(listOf("first", "second"), completed)
                    assertEquals(TaskState.Cancelled, first.currentState)
                    assertEquals(TaskState.Cancelled, second.currentState)
                    runCurrent()
                    queue.cancelAll()
                    assertEquals(listOf("first", "second"), completed)
                    assertTrue(queue.isIdle())
                    assertTrue(owner.isActive)
                } finally {
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }

    @Test fun handoffCancellationMayReenterFromBothCallbacksWithoutRepeatingCompletion() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            var armed = false
            var cancellationStarted = false
            var cancellations = 0
            var completions = 0
            var followingCompletions = 0
            val victim = Task<Unit>(
                block = { fail("Cancelled handoff work must not start") },
                onStateChange = {
                    if (it is TaskState.Cancelled) {
                        cancellations++
                        queue.cancelAll()
                    }
                },
                onComplete = {
                    completions++
                    queue.cancelAll()
                },
            )
            val following = Task<Unit>(
                block = { fail("Reentrant cancellation must cancel following work") },
                onStateChange = {
                    if (armed && !cancellationStarted && it is TaskState.Pending && queue.pending() == 1) {
                        cancellationStarted = true
                        assertTrue(queue.cancel(victim.id))
                    }
                },
                onComplete = { followingCompletions++ },
            )
            try {
                queue.enqueue(victim)
                queue.enqueue(following)
                armed = true
                runCurrent()

                assertTrue(cancellationStarted)
                assertEquals(1, cancellations)
                assertEquals(1, completions)
                assertEquals(1, followingCompletions)
                assertEquals(TaskState.Cancelled, victim.currentState)
                assertEquals(TaskState.Cancelled, following.currentState)
                assertEquals(0, queue.running())
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)

                val replacement = Task<Unit>(id = victim.id, block = { Unit })
                queue.enqueue(replacement)
                runCurrent()
                assertEquals(TaskState.Completed(true), replacement.currentState)
                assertEquals(1, completions)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test fun runningTasksCompleteOnlyAfterNonCancellableCleanupFinishes() = runTest {
        for (concurrent in listOf(false, true)) {
            for (shutdown in listOf(false, true)) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val queue = queue(owner, dispatcher, concurrent)
                val releaseCleanup = CompletableDeferred<Unit>()
                val events = mutableListOf<String>()
                var completions = 0
                val task = Task<Unit>(
                    block = {
                        events += "started"
                        try {
                            awaitCancellation()
                        } finally {
                            events += "cleaning"
                            withContext(NonCancellable) { releaseCleanup.await() }
                            events += "cleaned"
                        }
                    },
                    onComplete = { completions++; events += "complete" },
                )
                try {
                    queue.enqueue(task)
                    runCurrent()
                    assertEquals(listOf("started"), events)

                    if (shutdown) queue.shutdown() else queue.cancelAll()
                    runCurrent()
                    queue.cancelAll()
                    runCurrent()

                    assertEquals(listOf("started", "cleaning"), events)
                    assertEquals(0, completions)
                    assertEquals(1, queue.running())
                    assertFalse(queue.isIdle())
                    assertTrue(owner.isActive)

                    releaseCleanup.complete(Unit)
                    runCurrent()

                    assertEquals(listOf("started", "cleaning", "cleaned", "complete"), events)
                    assertEquals(1, completions)
                    assertEquals(TaskState.Cancelled, task.currentState)
                    assertEquals(0, queue.running())
                    assertTrue(queue.isIdle())
                } finally {
                    releaseCleanup.complete(Unit)
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
                assertEquals(1, completions)
            }
        }
    }

    @Test fun cancellingRegisteredJobsBeforeTheirBodiesStartCompletesExactlyOnce() = runTest {
        for (concurrent in listOf(false, true)) {
            for (shutdown in listOf(false, true)) {
                val owner = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
                val taskDispatcher = PausedTaskDispatcher()
                val queue = queue(owner, taskDispatcher, concurrent)
                var completions = 0
                val task = Task<Unit>(
                    block = { fail("Cancelled dispatched work must not start") },
                    onComplete = { completions++ },
                )
                try {
                    queue.enqueue(task)
                    runCurrent() // Register the execution Job, leaving its runnable on the other dispatcher.
                    assertEquals(0, queue.pending())
                    assertEquals(1, queue.running())
                    assertTrue(task.currentState is TaskState.Pending)
                    assertEquals(0, completions)

                    if (shutdown) queue.shutdown() else queue.cancelAll()
                    queue.cancelAll()
                    taskDispatcher.runAll()
                    runCurrent()

                    assertEquals(1, completions)
                    assertEquals(TaskState.Cancelled, task.currentState)
                    assertEquals(0, queue.running())
                    assertTrue(queue.isIdle())
                    assertTrue(owner.isActive)
                    if (!shutdown) {
                        val replacement = Task<Unit>(id = task.id, block = { Unit })
                        queue.enqueue(replacement)
                        runCurrent()
                        taskDispatcher.runAll()
                        runCurrent()
                        assertEquals(TaskState.Completed(true), replacement.currentState)
                        assertTrue(queue.isIdle())
                    }
                    assertEquals(1, completions)
                } finally {
                    queue.shutdown()
                    owner.cancel()
                    taskDispatcher.runAll()
                    runCurrent()
                }
            }
        }
    }

    @Test fun eachSequentialSubmissionOfTheSameTaskHasItsOwnCompletion() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            var executions = 0
            var completions = 0
            val task = Task<Unit>(
                block = { executions++ },
                onComplete = { completions++ },
            )
            try {
                repeat(2) { iteration ->
                    queue.enqueue(task)
                    assertTrue(task.currentState is TaskState.Pending)
                    assertEquals(iteration, completions)
                    runCurrent()
                    assertEquals(TaskState.Completed(true), task.currentState)
                    assertEquals(iteration + 1, executions)
                    assertEquals(iteration + 1, completions)
                    assertTrue(queue.isIdle())
                }

                queue.enqueue(task)
                assertTrue(task.currentState is TaskState.Pending)
                assertEquals(2, completions)
                assertTrue(queue.cancel(task.id))
                runCurrent()

                assertEquals(TaskState.Cancelled, task.currentState)
                assertEquals(2, executions)
                assertEquals(3, completions)
                assertTrue(queue.isIdle())
                queue.cancelAll()
                assertEquals(3, completions)
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test fun callerRunsCancelledByItsAdmissionStateCollectorStillCompletesOnce() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.callerRuns(1))
            var cancelledOnAdmission = false
            var completions = 0
            val caller = Task<Unit>(
                block = { fail("Caller work cancelled during admission must not start") },
                onComplete = { completions++ },
            )
            val collector = backgroundScope.launch(Dispatchers.Unconfined) {
                queue.state.collect {
                    if (!cancelledOnAdmission && queue.running() == 1) {
                        cancelledOnAdmission = true
                        queue.shutdown()
                    }
                }
            }
            try {
                queue.enqueue(Task<Unit>(block = { fail("Shutdown must cancel the pending filler") }))

                val result = queue.enqueue(caller)

                assertTrue(cancelledOnAdmission)
                assertTrue(result is EnqueueResult.CallerExecuted)
                assertFalse((result as EnqueueResult.CallerExecuted).success)
                assertTrue(result.error is CancellationException)
                assertEquals(TaskState.Cancelled, caller.currentState)
                assertEquals(1, completions)
                runCurrent()
                queue.cancelAll()
                assertEquals(1, completions)
                assertEquals(0, queue.running())
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                collector.cancel()
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test fun capacityRejectionsDoNotCompleteUnacceptedTasks() = runTest {
        for (concurrent in listOf(false, true)) {
            for (policy in listOf(RejectionPolicy.DISCARD, RejectionPolicy.THROW_EXCEPTION, RejectionPolicy.BLOCK)) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val queue = queue(owner, dispatcher, concurrent, BackpressureConfig(maxCapacity = 1, policy = policy))
                var completions = 0
                var rejectedResult: EnqueueResult? = null
                val rejected = Task<Unit>(
                    block = { fail("Rejected work must not start") },
                    onComplete = { completions++ },
                )
                val accepted = Task<Unit>(
                    block = { Unit },
                    onStateChange = {
                        // Exercise BLOCK through protected reentry rather than blocking the test thread.
                        if (policy == RejectionPolicy.BLOCK && it is TaskState.Pending && rejectedResult == null) {
                            rejectedResult = queue.enqueue(rejected)
                        }
                    },
                )
                try {
                    queue.enqueue(accepted)
                    if (policy != RejectionPolicy.BLOCK) rejectedResult = queue.enqueue(rejected)

                    assertTrue(rejectedResult is EnqueueResult.Rejected)
                    assertEquals(EnqueueResult.RejectionCause.CAPACITY, (rejectedResult as EnqueueResult.Rejected).cause)
                    assertEquals(0, completions)
                    queue.shutdown()
                    runCurrent()
                    assertEquals(0, completions)
                    assertTrue(owner.isActive)
                } finally {
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }

    @Test fun closedRejectionsDoNotCompleteUnacceptedTasks() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent)
            var completions = 0
            val rejected = Task<Unit>(
                block = { fail("Rejected work must not start") },
                onComplete = { completions++ },
            )
            try {
                queue.shutdown()

                val result = queue.enqueue(rejected)

                assertTrue(result is EnqueueResult.Rejected)
                assertEquals(EnqueueResult.RejectionCause.CLOSED, (result as EnqueueResult.Rejected).cause)
                assertEquals(TaskState.Cancelled, rejected.currentState)
                assertEquals(0, completions)
                runCurrent()
                assertEquals(0, completions)
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test fun serialClearKeepsItsSilentRemovalSemantics() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val owner = CoroutineScope(SupervisorJob() + dispatcher)
        val queue = SerialTaskQueue(owner, dispatcher)
        var completions = 0
        var cancellations = 0
        val task = Task<Unit>(
            block = { fail("Cleared work must not start") },
            onStateChange = { if (it is TaskState.Cancelled) cancellations++ },
            onComplete = { completions++ },
        )
        try {
            queue.enqueue(task)
            val pendingState = task.currentState

            queue.clear()
            runCurrent()
            queue.shutdown()

            assertEquals(pendingState, task.currentState)
            assertFalse(task.isCancelled)
            assertEquals(0, cancellations)
            assertEquals(0, completions)
            assertTrue(queue.isIdle)
            assertTrue(owner.isActive)
        } finally {
            queue.shutdown()
            owner.cancel()
            runCurrent()
        }
    }

    private fun failureCases(): List<FailureCase> {
        val stateCancellation = CancellationException("cancelled observer")
        val stateFatal = AssertionError("fatal cancelled observer")
        val firstCancellation = CancellationException("first cancellation")
        val secondCancellation = CancellationException("completion cancellation")
        val cancellationBeforeFatal = CancellationException("state before fatal completion")
        val fatalCompletion = LinkageError("fatal completion")
        val fatalBeforeCancellation = AssertionError("state before cancelled completion")
        val cancelledCompletion = CancellationException("cancelled completion")
        val firstFatal = AssertionError("first fatal")
        val secondFatal = LinkageError("second fatal")
        val repeated = CancellationException("same failure from both callbacks")
        return listOf(
            FailureCase(stateCancellation, null, stateCancellation, emptyList()),
            FailureCase(stateFatal, null, stateFatal, emptyList()),
            FailureCase(firstCancellation, secondCancellation, firstCancellation, listOf(secondCancellation)),
            FailureCase(cancellationBeforeFatal, fatalCompletion, fatalCompletion, listOf(cancellationBeforeFatal)),
            FailureCase(fatalBeforeCancellation, cancelledCompletion, fatalBeforeCancellation, listOf(cancelledCompletion)),
            FailureCase(firstFatal, secondFatal, firstFatal, listOf(secondFatal)),
            FailureCase(repeated, repeated, repeated, emptyList()),
        )
    }

    private data class FailureCase(
        val stateFailure: Throwable,
        val completionFailure: Throwable?,
        val expected: Throwable,
        val suppressed: List<Throwable>,
    )

    private enum class PendingAction {
        CANCEL, CANCEL_ALL, SHUTDOWN, DISCARD_OLDEST;

        fun config(): BackpressureConfig =
            if (this == DISCARD_OLDEST) BackpressureConfig.discardOldest(1) else BackpressureConfig.unbounded()

        fun apply(queue: QueueHarness, victim: Task<Unit>, replacement: Task<Unit>) {
            when (this) {
                CANCEL -> assertTrue(queue.cancel(victim.id))
                CANCEL_ALL -> queue.cancelAll()
                SHUTDOWN -> queue.shutdown()
                DISCARD_OLDEST -> assertTrue(queue.enqueue(replacement) is EnqueueResult.Discarded)
            }
        }
    }

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig = BackpressureConfig.unbounded(),
    ): QueueHarness = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueHarness(queue::enqueueWithResult, queue::cancel, queue::cancelAll, queue::shutdown,
            { queue.pendingTaskCount }, { queue.runningTaskCount }, { queue.isIdle }, queue.state)
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config)
        QueueHarness(queue::enqueueWithResult, queue::cancel, queue::cancelAll, queue::shutdown,
            { queue.pendingCount }, { queue.runningTaskCount }, { queue.isIdle }, queue.state)
    }

    private class QueueHarness(
        val enqueue: (Task<Unit>) -> EnqueueResult,
        val cancel: (String) -> Boolean,
        val cancelAll: () -> Unit,
        val shutdown: () -> Unit,
        val pending: () -> Int,
        val running: () -> Int,
        val isIdle: () -> Boolean,
        val state: StateFlow<*>,
    )

    private class PausedTaskDispatcher : CoroutineDispatcher() {
        private val work = ConcurrentLinkedQueue<Runnable>()

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            work.add(block)
        }

        fun runAll() {
            while (true) (work.poll() ?: return).run()
        }
    }
}
