package com.cn.tasks

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class BackpressureConfigTest {
    @Test fun negativeAndEmptySizesHaveZeroUsage() {
        val config = BackpressureConfig(maxCapacity = 3)

        for (size in listOf(Int.MIN_VALUE, -1, 0)) {
            assertEquals(0, config.calculateUsagePercent(size))
            assertFalse(config.isThresholdReached(size))
        }
    }

    @Test fun usageWithinCapacityRetainsIntegerPercentageTruncation() {
        val config = BackpressureConfig(maxCapacity = 3)

        assertEquals(33, config.calculateUsagePercent(1))
        assertEquals(66, config.calculateUsagePercent(2))
        assertEquals(100, config.calculateUsagePercent(3))
    }

    @Test fun fullAndOversizedInputsStayAtOneHundredWithoutOverflow() {
        val singleSlot = BackpressureConfig(maxCapacity = 1)
        for (size in listOf(1, 2, Int.MAX_VALUE)) {
            assertEquals(100, singleSlot.calculateUsagePercent(size))
            assertTrue(singleSlot.isThresholdReached(size))
        }

        val large = BackpressureConfig(maxCapacity = Int.MAX_VALUE - 1)
        assertEquals(50, large.calculateUsagePercent((Int.MAX_VALUE - 1) / 2))
        assertEquals(100, large.calculateUsagePercent(Int.MAX_VALUE - 1))
        assertEquals(100, large.calculateUsagePercent(Int.MAX_VALUE))
    }

    @Test fun thresholdChecksUseTheClampedUsageAndExistingIntegerBoundary() {
        val rounded = BackpressureConfig(maxCapacity = 3, warnThreshold = 67)
        assertFalse(rounded.isThresholdReached(2))
        assertTrue(rounded.isThresholdReached(3))
        assertTrue(rounded.isThresholdReached(Int.MAX_VALUE))

        val fullOnly = BackpressureConfig(maxCapacity = 100, warnThreshold = 100)
        assertFalse(fullOnly.isThresholdReached(99))
        assertTrue(fullOnly.isThresholdReached(100))
        assertTrue(fullOnly.isThresholdReached(101))

        val zeroThreshold = BackpressureConfig(maxCapacity = 3, warnThreshold = 0)
        assertTrue(zeroThreshold.isThresholdReached(Int.MIN_VALUE))
        assertTrue(zeroThreshold.isThresholdReached(0))
    }

    @Test fun unboundedConfigurationAlwaysDisablesUsageAndThresholdWarnings() {
        val config = BackpressureConfig.unbounded().copy(warnThreshold = 0)

        for (size in listOf(Int.MIN_VALUE, -1, 0, 1, Int.MAX_VALUE)) {
            assertEquals(0, config.calculateUsagePercent(size))
            assertFalse(config.isThresholdReached(size))
        }
    }
}
