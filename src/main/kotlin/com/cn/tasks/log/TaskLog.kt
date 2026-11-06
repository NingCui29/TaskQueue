package com.cn.tasks.log

import kotlinx.coroutines.CancellationException

/**
 * 队列内部诊断输出的接缝。
 *
 * 队列需要输出少量内部诊断信息，但不该为此绑定任何具体日志框架。
 * 这里只保留队列自身与文档示例用到的三个入口，优先级取值沿用 `android.util.Log`，
 * 宿主把 [printer] 指向自己的日志实现即可接管，队列不因此引入任何依赖。
 *
 * [enabled] 默认 **false**：库无法感知宿主的构建类型，
 * 是否输出必须由宿主显式决定，避免误开日志。
 */
object TaskLog {

    /** 调试级，取值同 `android.util.Log.DEBUG`。 */
    const val PRIORITY_DEBUG = 3

    /** 警告级，取值同 `android.util.Log.WARN`。 */
    const val PRIORITY_WARN = 5

    /** 错误级，取值同 `android.util.Log.ERROR`。 */
    const val PRIORITY_ERROR = 6

    /** 未显式传入标签时使用的默认标签。 */
    const val DEFAULT_TAG = "TaskQueue"

    /**
     * 日志输出端。默认丢弃，宿主可替换为 logcat、文件或远端上报。
     * 输出端的普通异常会被隔离；取消异常与致命错误仍向上传播。
     */
    fun interface Printer {
        fun print(priority: Int, tag: String, message: String, throwable: Throwable?)
    }

    /** 全局开关，由宿主在启动时按自身构建类型打开。 */
    @Volatile
    var enabled: Boolean = false

    /** 低于该级别的日志直接丢弃。 */
    @Volatile
    var minPriority: Int = PRIORITY_DEBUG

    @Volatile
    var printer: Printer = Printer { _, _, _, _ -> }

    /** 调试输出，消息已知时使用；拼接开销较大时改用下方的惰性求值版本。 */
    fun d(message: String, tag: String = DEFAULT_TAG, throwable: Throwable? = null) {
        if (isLoggable(PRIORITY_DEBUG)) print(PRIORITY_DEBUG, tag, message, throwable)
    }

    /**
     * 惰性求值的调试输出：仅在确实会输出时才构造消息串。
     *
     * 用于拼接开销较大的场景，例如 `TaskLog.d { queue.snapshot().joinToString() }`——
     * 关闭日志时完全不会触发拼接。
     */
    inline fun d(tag: String = DEFAULT_TAG, throwable: Throwable? = null, message: () -> String) {
        if (isLoggable(PRIORITY_DEBUG)) print(PRIORITY_DEBUG, tag, message(), throwable)
    }

    /** 警告输出，用于入队被拒绝、旧任务被淘汰等可预期但值得关注的情况。 */
    fun w(message: String, tag: String = DEFAULT_TAG, throwable: Throwable? = null) {
        if (isLoggable(PRIORITY_WARN)) print(PRIORITY_WARN, tag, message, throwable)
    }

    /** 错误输出，用于回调抛异常、通知失败等不应发生的情况。 */
    fun e(message: String, tag: String = DEFAULT_TAG, throwable: Throwable? = null) {
        if (isLoggable(PRIORITY_ERROR)) print(PRIORITY_ERROR, tag, message, throwable)
    }

    /** 供 inline 函数做前置判断，避免无谓的字符串构造。 */
    @PublishedApi
    internal fun isLoggable(priority: Int): Boolean = enabled && priority >= minPriority

    @PublishedApi
    internal fun print(priority: Int, tag: String, message: String, throwable: Throwable?) {
        try {
            printer.print(priority, tag, message, throwable)
        } catch (error: CancellationException) {
            throw error
        } catch (_: Exception) {
            // 诊断输出失败不能改变任务结果；此处不能再调用日志，以免递归失败。
        }
    }
}
