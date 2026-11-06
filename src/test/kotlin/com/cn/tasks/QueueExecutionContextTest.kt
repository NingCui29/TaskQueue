package com.cn.tasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class QueueExecutionContextTest {
    @Test fun fatalNotificationFailureAfterCancellationIsNeverDowngraded() = runTest {
        val context = QueueExecutionContext()
        val cancellation = CancellationException("first observer cancelled")
        val fatal = AssertionError("second observer failed fatally")

        val failure = runCatching {
            context.notifyForDispatch {
                context.notifyAll(listOf(
                    { throw cancellation },
                    { throw fatal },
                ))
            }
        }.exceptionOrNull()

        assertSame(fatal, failure)
        assertFalse(context.isEntered)
        assertTrue(isActive)
    }

    @Test fun dispatchNotificationIsolatesCallbackCancellationWhileCallerRemainsActive() = runTest {
        val context = QueueExecutionContext()

        context.notifyForDispatch {
            assertTrue(context.isEntered)
            throw CancellationException("observer callback cancelled")
        }

        assertFalse(context.isEntered)
        assertTrue(isActive)
    }

    @Test fun dispatchNotificationPropagatesTheCallersActualCancellation() = runTest {
        val context = QueueExecutionContext()
        val owner = Job()
        val ownerCancellation = CancellationException("real owner cancellation")
        val callbackCancellation = CancellationException("observer callback cancelled")
        var reachedAfterNotification = false
        val isolatedScope = CoroutineScope(coroutineContext + owner)
        try {
            val execution = isolatedScope.async(start = CoroutineStart.UNDISPATCHED) {
                context.notifyForDispatch {
                    owner.cancel(ownerCancellation)
                    throw callbackCancellation
                }
                reachedAfterNotification = true
            }
            val failure = runCatching { execution.await() }.exceptionOrNull()

            assertTrue(execution.isCancelled)
            assertTrue(failure is CancellationException)
            assertNotSame(callbackCancellation, failure)
            assertFalse(reachedAfterNotification)
            assertFalse(context.isEntered)
            assertTrue(isActive)
        } finally {
            owner.cancel()
        }
    }

    @Test fun nestedNotificationFailureRestoresEachQueuesThreadLocalState() {
        val first = QueueExecutionContext()
        val second = QueueExecutionContext()
        val expected = IllegalStateException("nested observer failed")

        assertFalse(first.isEntered)
        assertFalse(second.isEntered)
        first.notify {
            assertTrue(first.isEntered)
            assertFalse(second.isEntered)
            val failure = runCatching {
                second.notify {
                    assertTrue(first.isEntered)
                    assertTrue(second.isEntered)
                    first.notify {
                        assertTrue(first.isEntered)
                        assertTrue(second.isEntered)
                        throw expected
                    }
                }
            }.exceptionOrNull()

            assertSame(expected, failure)
            assertTrue(first.isEntered)
            assertFalse(second.isEntered)
        }
        assertFalse(first.isEntered)
        assertFalse(second.isEntered)

        val failure = runCatching { first.notify { throw expected } }.exceptionOrNull()
        assertSame(expected, failure)
        assertFalse(first.isEntered)
        assertFalse(second.isEntered)
    }
}
