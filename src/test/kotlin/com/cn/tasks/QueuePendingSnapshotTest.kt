package com.cn.tasks

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class QueuePendingSnapshotTest {
    @Test fun reentrantInsertionCannotRestoreOlderPendingPositionOrSize() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent)
            var inserted = false
            val third = Task(priority = TaskPriority.HIGH, block = { Unit })
            val first = Task(block = { Unit }, onStateChange = { state ->
                if (state is TaskState.Pending && state.queueSize == 2 && !inserted) {
                    inserted = true
                    queue.enqueue(third)
                }
            })
            val secondStates = mutableListOf<TaskState>()
            val second = Task(block = { Unit }, onStateChange = { secondStates += it })
            try {
                queue.enqueue(first)
                queue.enqueue(second)

                assertTrue(inserted)
                assertEquals(3, queue.pending())
                assertEquals(TaskState.Pending(2, 3), first.currentState)
                assertEquals(TaskState.Pending(3, 3), second.currentState)
                assertEquals(listOf(TaskState.Pending(3, 3)), secondStates)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun reentrantCancellationCannotRestoreTheCancelledTasksPendingState() = runTest {
        for (concurrent in listOf(false, true)) {
            val queue = queue(this, StandardTestDispatcher(testScheduler), concurrent)
            val secondStates = mutableListOf<TaskState>()
            val second = Task(block = { Unit }, onStateChange = { secondStates += it })
            var removed = false
            val first = Task(block = { Unit }, onStateChange = { state ->
                if (state is TaskState.Pending && state.queueSize == 2 && !removed) {
                    removed = true
                    assertTrue(queue.cancel(second.id))
                }
            })
            try {
                queue.enqueue(first)
                queue.enqueue(second)

                assertTrue(removed)
                assertEquals(1, queue.pending())
                assertEquals(TaskState.Pending(1, 1), first.currentState)
                assertEquals(TaskState.Cancelled, second.currentState)
                assertEquals(listOf(TaskState.Cancelled), secondStates)
            } finally {
                queue.shutdown()
                runCurrent()
            }
        }
    }

    @Test fun preparedPendingUpdatesIgnoreOlderVersionsAndNeverReviveTerminalTasks() {
        val states = mutableListOf<TaskState>()
        val task = Task(block = { Unit }, onStateChange = { states += it })
        val older = task.preparePendingState(TaskState.Pending(2, 2))
        val newer = task.preparePendingState(TaskState.Pending(3, 3))

        newer()
        older()

        assertEquals(TaskState.Pending(3, 3), task.currentState)
        assertEquals(listOf(TaskState.Pending(3, 3)), states)

        val pendingBeforeRunning = task.preparePendingState(TaskState.Pending(1, 1))
        task.updateState(TaskState.Running())
        pendingBeforeRunning()
        assertEquals(TaskState.Running(), task.currentState)

        val pendingBeforeCompletion = task.preparePendingState(TaskState.Pending(1, 1))
        task.updateState(TaskState.Completed(true))
        pendingBeforeCompletion()
        assertEquals(TaskState.Completed(true), task.currentState)
        assertEquals(
            listOf(TaskState.Pending(3, 3), TaskState.Running(), TaskState.Completed(true)),
            states,
        )
    }

    private fun queue(
        owner: CoroutineScope,
        dispatcher: CoroutineDispatcher,
        concurrent: Boolean,
    ): QueueAccess = if (concurrent) {
        val queue = ConcurrentTaskQueue(owner, 1, dispatcher)
        QueueAccess({ queue.enqueue(it) }, queue::cancel, queue::shutdown, { queue.pendingTaskCount })
    } else {
        val queue = SerialTaskQueue(owner, dispatcher).apply { pause() }
        QueueAccess({ queue.enqueue(it) }, queue::cancel, queue::shutdown, { queue.pendingCount })
    }

    private class QueueAccess(
        val enqueue: (Task<Unit>) -> Unit,
        val cancel: (String) -> Boolean,
        val shutdown: () -> Unit,
        val pending: () -> Int,
    )
}
