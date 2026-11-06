package com.cn.tasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueueCallbackFailureTest {
    @Test fun pendingCallbackFailureDoesNotStrandAnAcceptedTask() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, BackpressureConfig.unbounded())
            val failure = CancellationException("pending callback")
            var ran = false
            val task = Task(
                block = { ran = true },
                onStateChange = { if (it is TaskState.Pending) throw failure },
            )
            try {
                assertSame(failure, runCatching { queue.enqueue(task) }.exceptionOrNull())
                runCurrent()
                assertTrue(ran)
                assertEquals(TaskState.Completed(success = true), task.currentState)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun thresholdCallbackFailureDoesNotStrandAnAcceptedTask() = runTest {
        for (concurrent in listOf(false, true)) {
            val failure = IllegalStateException("threshold callback")
            val config = BackpressureConfig(maxCapacity = 1, onThresholdReached = { _, _, _ -> throw failure })
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent, config)
            var ran = false
            val task = Task(block = { ran = true })
            try {
                assertSame(failure, runCatching { queue.enqueue(task) }.exceptionOrNull())
                runCurrent()
                assertTrue(ran)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun cancellingAndClearingAPausedSerialQueuePublishItsNewPendingCount() = runTest {
        val queue = SerialTaskQueue(this, StandardTestDispatcher(testScheduler))
        try {
            queue.pause()
            val task = queue.enqueue(Task(block = { Unit }))
            assertEquals(SerialTaskQueue.QueueState.Paused(1), queue.state.value)
            assertTrue(queue.cancel(task.id))
            assertEquals(SerialTaskQueue.QueueState.Paused(0), queue.state.value)

            queue.enqueue(Task(block = { Unit }))
            queue.clear()

            assertEquals(SerialTaskQueue.QueueState.Paused(0), queue.state.value)
        } finally {
            queue.shutdown()
        }
    }

    @Test fun managerCancellationVisitsEveryQueueBeforeRethrowingCallbackFailures() = runTest {
        TaskManager.initialize(this)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val first = TaskManager.createSerialQueue("failing_cancel_first", dispatcher).apply { pause() }
        val second = TaskManager.createSerialQueue("failing_cancel_second", dispatcher).apply { pause() }
        val firstTask = cancelledCallbackTask("first")
        val secondTask = cancelledCallbackTask("second")
        first.enqueue(firstTask)
        second.enqueue(secondTask)
        try {
            val failure = runCatching { TaskManager.cancelAll() }.exceptionOrNull()

            assertTrue(failure is CancellationException)
            assertEquals(1, requireNotNull(failure).suppressed.size)
            assertTrue(firstTask.isCancelled)
            assertTrue(secondTask.isCancelled)
            assertEquals(0, first.pendingCount)
            assertEquals(0, second.pendingCount)
        } finally {
            TaskManager.shutdown()
            runCurrent()
        }
    }

    @Test fun managerCancelsTheSameIdInLaterQueuesAfterACallbackFailure() = runTest {
        TaskManager.initialize(this)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val first = TaskManager.createSerialQueue("same_id_serial", dispatcher).apply { pause() }
        val second = TaskManager.createConcurrentQueue("same_id_concurrent", dispatcher = dispatcher)
        val failure = CancellationException("first cancelled callback")
        val taskId = "shared_id"
        val firstTask = Task(
            id = taskId,
            block = { Unit },
            onStateChange = { if (it is TaskState.Cancelled) throw failure },
        )
        val secondTask = Task(id = taskId, block = { Unit })
        first.enqueue(firstTask)
        second.enqueue(secondTask)
        try {
            assertSame(failure, runCatching { TaskManager.cancelTask(taskId) }.exceptionOrNull())
            assertTrue(firstTask.isCancelled)
            assertTrue(secondTask.isCancelled)
            assertEquals(0, first.pendingCount)
            assertEquals(0, second.pendingTaskCount)
            assertEquals(false, TaskManager.cancelTask(taskId))

            first.enqueue(Task(id = taskId, block = { Unit }))
            assertTrue(TaskManager.cancelTask(taskId))
        } finally {
            TaskManager.shutdown()
            runCurrent()
        }
    }

    @Test fun cancellationCallbackFailureStillPublishesTheConcurrentPendingCount() = runTest {
        val queue = ConcurrentTaskQueue(this, 1, StandardTestDispatcher(testScheduler))
        val failure = CancellationException("cancelled state callback")
        val task = Task(
            block = { Unit },
            onStateChange = { if (it is TaskState.Cancelled) throw failure },
        )
        queue.enqueue(Task(block = { Unit }))
        queue.enqueue(task)
        try {
            assertSame(failure, runCatching { queue.cancel(task.id) }.exceptionOrNull())
            assertEquals(ConcurrentTaskQueue.ConcurrentState.Running(0, 1), queue.state.value)
        } finally {
            queue.shutdown()
            runCurrent()
        }
    }

    private fun cancelledCallbackTask(name: String) = Task(
        name = name,
        block = { Unit },
        onStateChange = { if (it is TaskState.Cancelled) throw CancellationException(name) },
    )

    private fun queue(
        owner: CoroutineScope,
        dispatcher: kotlinx.coroutines.CoroutineDispatcher,
        concurrent: Boolean,
        config: BackpressureConfig,
    ): QueueHarness = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher, config)
        QueueHarness({ queue.enqueue(it); Unit }, queue::shutdown)
    } else {
        val queue = SerialTaskQueue(owner, dispatcher, config)
        QueueHarness({ queue.enqueue(it); Unit }, queue::shutdown)
    }

    private class QueueHarness(val enqueue: (Task<Unit>) -> Unit, val shutdown: () -> Unit)
}
