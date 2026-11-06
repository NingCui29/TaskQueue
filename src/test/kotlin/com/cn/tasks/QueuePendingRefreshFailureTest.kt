package com.cn.tasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

/** 验证取消及淘汰通知失败后，已变更队列仍刷新等待位次并继续调度。 */
@OptIn(ExperimentalCoroutinesApi::class)
class QueuePendingRefreshFailureTest {
    @Test
    fun failedCancellationStillRefreshesPositionsAndRemainingTasksRun() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.unbounded())
            val failure = CancellationException("cancelled observer")
            var armed = false
            val events = mutableListOf<String>()
            val removed = Task<Unit>(block = { events += "removed" }, onStateChange = {
                if (armed && it is TaskState.Cancelled) throw failure
            })
            val first = Task<Unit>(block = { events += "first" })
            val second = Task<Unit>(block = { events += "second" })
            try {
                queue.enqueue(removed)
                queue.enqueue(first)
                queue.enqueue(second)
                armed = true

                assertSame(failure, runCatching { queue.cancel(removed.id) }.exceptionOrNull())

                assertEquals(TaskState.Cancelled, removed.currentState)
                assertPending(queue, first, second)
                assertEquals(emptyList<String>(), events)
                armed = false
                queue.resume()
                runCurrent()
                assertEquals(listOf("first", "second"), events)
                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Completed(true), second.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                armed = false
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test
    fun normalCancellationRefreshesPositionsBeforeDispatch() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig.unbounded())
            val removed = Task<Unit>(block = { Unit })
            val first = Task<Unit>(block = { Unit })
            val second = Task<Unit>(block = { Unit })
            try {
                queue.enqueue(removed)
                queue.enqueue(first)
                queue.enqueue(second)

                assertTrue(queue.cancel(removed.id))

                assertEquals(TaskState.Cancelled, removed.currentState)
                assertPending(queue, first, second)
                queue.resume()
                runCurrent()
                assertEquals(TaskState.Completed(true), first.currentState)
                assertEquals(TaskState.Completed(true), second.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test
    fun failedDiscardNotificationRefreshesPositionsAndAcceptedTaskRuns() = runTest {
        for (concurrent in listOf(false, true)) {
            for (cancelledObserver in listOf(false, true)) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val failure = if (cancelledObserver) CancellationException("cancelled victim observer")
                    else IllegalStateException("discarded observer")
                var armed = false
                val queue = queue(owner, dispatcher, concurrent, BackpressureConfig(
                    maxCapacity = 2,
                    policy = RejectionPolicy.DISCARD_OLDEST,
                    onDiscarded = { _, _ -> if (armed && !cancelledObserver) throw failure },
                ))
                val events = mutableListOf<String>()
                val removed = Task<Unit>(block = { events += "removed" }, onStateChange = {
                    if (armed && cancelledObserver && it is TaskState.Cancelled) throw failure
                })
                val survivor = Task<Unit>(block = { events += "survivor" })
                val accepted = Task<Unit>(block = { events += "accepted" })
                try {
                    queue.enqueue(removed)
                    queue.enqueue(survivor)
                    armed = true

                    assertSame(failure, runCatching { queue.enqueue(accepted) }.exceptionOrNull())

                    assertEquals(TaskState.Cancelled, removed.currentState)
                    assertPending(queue, survivor, accepted)
                    assertEquals(emptyList<String>(), events)
                    armed = false
                    queue.resume()
                    runCurrent()
                    assertEquals(listOf("survivor", "accepted"), events)
                    assertEquals(TaskState.Completed(true), survivor.currentState)
                    assertEquals(TaskState.Completed(true), accepted.currentState)
                    assertTrue(queue.isIdle())
                    assertTrue(owner.isActive)
                } finally {
                    armed = false
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }

    @Test
    fun successfulDiscardPublishesEachPendingPositionOnceInTheExistingCallbackOrder() = runTest {
        for (concurrent in listOf(false, true)) {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val owner = CoroutineScope(SupervisorJob() + dispatcher)
            var armed = false
            val notifications = mutableListOf<String>()
            val executed = mutableListOf<String>()
            var survivorPending = 0
            var acceptedPending = 0
            val queue = queue(owner, dispatcher, concurrent, BackpressureConfig(
                maxCapacity = 2,
                policy = RejectionPolicy.DISCARD_OLDEST,
                onDiscarded = { _, _ -> if (armed) notifications += "discarded" },
                onThresholdReached = { size, capacity, percent ->
                    if (armed) notifications += "threshold:$size/$capacity/$percent"
                },
            ))
            val removed = Task<Unit>(block = { executed += "removed" }, onStateChange = {
                if (armed && it is TaskState.Cancelled) notifications += "cancelled"
            })
            val survivor = Task<Unit>(block = { executed += "survivor" }, onStateChange = {
                if (armed && it is TaskState.Pending) {
                    survivorPending++
                    notifications += "survivor-pending"
                }
            })
            val accepted = Task<Unit>(block = { executed += "accepted" }, onStateChange = {
                if (armed && it is TaskState.Pending) {
                    acceptedPending++
                    notifications += "accepted-pending"
                }
            })
            try {
                queue.enqueue(removed)
                queue.enqueue(survivor)
                notifications.clear()
                armed = true

                queue.enqueue(accepted)

                assertEquals(listOf("cancelled", "discarded", "survivor-pending",
                    "accepted-pending", "threshold:2/2/100"), notifications)
                assertEquals(1, survivorPending)
                assertEquals(1, acceptedPending)
                assertEquals(TaskState.Cancelled, removed.currentState)
                assertPending(queue, survivor, accepted)
                armed = false
                queue.resume()
                runCurrent()
                assertEquals(listOf("survivor", "accepted"), executed)
                assertEquals(TaskState.Completed(true), survivor.currentState)
                assertEquals(TaskState.Completed(true), accepted.currentState)
                assertTrue(queue.isIdle())
                assertTrue(owner.isActive)
            } finally {
                armed = false
                queue.shutdown()
                owner.cancel()
                runCurrent()
            }
        }
    }

    @Test
    fun mutationAndRefreshFailuresPreserveOriginalErrorsAndFatalPriority() = runTest {
        for (concurrent in listOf(false, true)) {
            for (discard in listOf(false, true)) {
                for (failureCase in 0..4) {
                    val dispatcher = StandardTestDispatcher(testScheduler)
                    val owner = CoroutineScope(SupervisorJob() + dispatcher)
                    val initialFailure: Throwable = when (failureCase) {
                        2, 3 -> AssertionError("original fatal notification")
                        4 -> CancellationException("repeated cancellation")
                        else -> if (discard) IllegalStateException("discarded notification")
                            else CancellationException("cancelled notification")
                    }
                    val refreshFailure: Throwable = when (failureCase) {
                        1, 3 -> LinkageError("fatal pending notification")
                        4 -> initialFailure
                        else -> CancellationException("pending notification")
                    }
                    var armed = false
                    val config = if (discard) BackpressureConfig(
                        maxCapacity = 2,
                        policy = RejectionPolicy.DISCARD_OLDEST,
                        onDiscarded = { _, _ -> if (armed) throw initialFailure },
                    ) else BackpressureConfig.unbounded()
                    val queue = queue(owner, dispatcher, concurrent, config)
                    val removed = Task<Unit>(block = { Unit }, onStateChange = {
                        if (armed && !discard && it is TaskState.Cancelled) throw initialFailure
                    })
                    val first = Task<Unit>(block = { Unit })
                    val last = Task<Unit>(block = { Unit }, onStateChange = {
                        // 致命错误放在本批最后一项，不要求致命错误之后还继续派发通知。
                        if (armed && it is TaskState.Pending) throw refreshFailure
                    })
                    try {
                        queue.enqueue(removed)
                        queue.enqueue(first)
                        if (!discard) queue.enqueue(last)
                        armed = true

                        val reported = runCatching {
                            if (discard) queue.enqueue(last) else queue.cancel(removed.id)
                        }.exceptionOrNull()

                        // 后发生的致命错误优先；其他情况保留首个错误，同实例不自抑制。
                        val expected = if (failureCase == 1) refreshFailure else initialFailure
                        val secondary = if (failureCase == 1) initialFailure else refreshFailure
                        assertSame(expected, reported)
                        assertEquals(if (failureCase == 4) emptyList() else listOf(secondary), expected.suppressed.toList())
                        assertEquals(TaskState.Cancelled, removed.currentState)
                        assertPending(queue, first, last)
                        armed = false
                        queue.resume()
                        runCurrent()
                        assertEquals(TaskState.Completed(true), first.currentState)
                        assertEquals(TaskState.Completed(true), last.currentState)
                        assertTrue(queue.isIdle())
                        assertTrue(owner.isActive)
                    } finally {
                        armed = false
                        queue.shutdown()
                        owner.cancel()
                        runCurrent()
                    }
                }
            }
        }
    }

    @Test
    fun failedNotificationReentryRefreshesTheLatestMembershipWithoutRevivingRemovedTasks() = runTest {
        for (concurrent in listOf(false, true)) {
            for (discard in listOf(false, true)) {
                val dispatcher = StandardTestDispatcher(testScheduler)
                val owner = CoroutineScope(SupervisorJob() + dispatcher)
                val failure = CancellationException("notification after reentry")
                var armed = false
                var reentered = false
                val events = mutableListOf<String>()
                val sibling = Task<Unit>(block = { events += "sibling" })
                val survivor = Task<Unit>(block = { events += "survivor" })
                val accepted = Task<Unit>(block = { events += "accepted" })
                val inserted = Task<Unit>(priority = TaskPriority.HIGH, block = { events += "inserted" })
                lateinit var queue: QueueAccess
                fun mutateFromNotification() {
                    reentered = true
                    assertTrue(queue.cancel(sibling.id))
                    queue.enqueue(inserted)
                    throw failure
                }
                val config = if (discard) BackpressureConfig(
                    maxCapacity = 3,
                    policy = RejectionPolicy.DISCARD_OLDEST,
                    onDiscarded = { _, _ -> if (armed && !reentered) mutateFromNotification() },
                ) else BackpressureConfig.unbounded()
                queue = queue(owner, dispatcher, concurrent, config)
                val removed = Task<Unit>(block = { events += "removed" }, onStateChange = {
                    if (armed && !discard && !reentered && it is TaskState.Cancelled) mutateFromNotification()
                })
                try {
                    queue.enqueue(removed)
                    queue.enqueue(sibling)
                    queue.enqueue(survivor)
                    armed = true

                    assertSame(failure, runCatching {
                        if (discard) queue.enqueue(accepted) else queue.cancel(removed.id)
                    }.exceptionOrNull())

                    assertTrue(reentered)
                    assertEquals(TaskState.Cancelled, removed.currentState)
                    assertEquals(TaskState.Cancelled, sibling.currentState)
                    if (discard) assertPending(queue, inserted, survivor, accepted)
                    else assertPending(queue, inserted, survivor)
                    armed = false
                    queue.resume()
                    runCurrent()
                    assertEquals(if (discard) listOf("inserted", "survivor", "accepted")
                        else listOf("inserted", "survivor"), events)
                    assertEquals(TaskState.Cancelled, removed.currentState)
                    assertEquals(TaskState.Cancelled, sibling.currentState)
                    assertTrue(queue.isIdle())
                    assertTrue(owner.isActive)
                } finally {
                    armed = false
                    queue.shutdown()
                    owner.cancel()
                    runCurrent()
                }
            }
        }
    }

    private fun assertPending(queue: QueueAccess, vararg tasks: Task<Unit>) {
        assertEquals(tasks.size, queue.pending())
        assertEquals(tasks.size, queue.statePending())
        tasks.forEachIndexed { index, task ->
            assertEquals(TaskState.Pending(index + 1, tasks.size), task.currentState)
        }
    }

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig,
    ): QueueAccess = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueAccess({ queue.enqueue(it); Unit }, queue::cancel, queue::shutdown, {},
            { queue.pendingTaskCount }, { queue.isIdle }) {
            when (val state = queue.state.value) {
                is ConcurrentTaskQueue.ConcurrentState.Running -> state.pendingCount
                is ConcurrentTaskQueue.ConcurrentState.Full -> state.pendingCount
                else -> -1
            }
        }
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config).apply { pause() }
        QueueAccess({ queue.enqueue(it); Unit }, queue::cancel, queue::shutdown, queue::resume,
            { queue.pendingCount }, { queue.isIdle }) {
            (queue.state.value as SerialTaskQueue.QueueState.Paused).pendingCount
        }
    }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> Unit,
        val cancel: (String) -> Boolean,
        val shutdown: () -> Unit,
        val resume: () -> Unit,
        val pending: () -> Int,
        val isIdle: () -> Boolean,
        val statePending: () -> Int,
    )
}
