# TaskQueue

纯 Kotlin/JVM 的协程任务队列库，提供有序队列、并发队列、优先级调度、背压策略与完整的生命周期语义。不依赖 Android、不依赖任何 DI 框架。

坐标：`com.cn.tasks:TaskQueue:0.1.0`

## 环境要求

| 项 | 值 |
| --- | --- |
| JDK | 17（由 `gradle/gradle-daemon-jvm.properties` 固定） |
| Kotlin | 2.1.20 |
| kotlinx-coroutines | 1.8.1 |
| 字节码目标 | JVM 17 |

唯一编译期依赖是 `kotlinx-coroutines-core`，且以 `api` 暴露——`CoroutineScope`、`CoroutineDispatcher`、`Flow` 都出现在公开签名上。

## 快速开始

队列不创建自己的根作用域，必须由宿主注入一个生命周期覆盖整个进程的 `CoroutineScope`。未注入就提交任务会立即报错，而不是静默使用一份游离作用域。

```kotlin
// 宿主启动期注入一次
TaskManager.initialize(applicationScope)

// 提交有序任务
TaskManager.submitSerial(
    priority = TaskPriority.HIGH,
    block = { progress -> fetchData(progress) },
    onSuccess = { result -> handleResult(result) },
    onStateChange = { state -> handleState(state) },
)

// 提交并发任务
TaskManager.submitConcurrent(
    priority = TaskPriority.NORMAL,
    block = { progress -> downloadImage(url, progress) },
    onSuccess = { bitmap -> displayImage(bitmap) },
)
```

需要独立生命周期时改用 `TaskQueueScope`，它的 `shutdown()` 只取消自身在途任务，绝不触碰注入进来的共享作用域：

```kotlin
val scope = TaskQueueScope.serial(applicationScope, "my_serial")
scope.enqueue(block = { progress -> task1(progress) })
scope.shutdown()
```

也可以直接持有队列，自行决定调度器与背压：

```kotlin
val queue = SerialTaskQueue(
    scope = applicationScope,
    dispatcher = Dispatchers.IO,
    backpressureConfig = BackpressureConfig.discardOldest(maxCapacity = 64),
)
val result = queue.enqueueWithResult(block = { doWork() })
when (result) {
    is EnqueueResult.Success -> TaskLog.d("已入队: ${result.task.id}", TAG)
    is EnqueueResult.Rejected -> TaskLog.w("被拒绝: ${result.reason}", TAG)
    is EnqueueResult.Discarded -> TaskLog.w("淘汰旧任务: ${result.discardedTask.name}", TAG)
    is EnqueueResult.CallerExecuted -> TaskLog.d("已在调用者线程执行", TAG)
}
```

## 日志接缝

库内部只有少量诊断输出，全部走 `com.cn.tasks.log.TaskLog`。它默认 `enabled = false`——库无法感知宿主的构建类型，是否输出必须由宿主显式决定：

```kotlin
TaskLog.enabled = BuildConfig.DEBUG
TaskLog.printer = TaskLog.Printer { priority, tag, message, throwable ->
    android.util.Log.println(priority, tag, message)
    throwable?.let { android.util.Log.println(priority, tag, android.util.Log.getStackTraceString(it)) }
}
```

优先级常量 `PRIORITY_DEBUG` / `PRIORITY_WARN` / `PRIORITY_ERROR` 取值与 `android.util.Log` 一致，便于 Android 宿主直接转接。拼接开销大的调用点使用惰性重载 `TaskLog.d(TAG) { "..." }`，关闭日志时完全不会触发字符串构造。

## 构建与验证

```bash
./gradlew.bat build                   # 编译 + 单测（可能命中缓存，见下）
./gradlew.bat test --rerun-tasks      # 强制真实执行全部单测
./gradlew.bat publishToMavenLocal
```

当前状态：26 个测试类、116 个测试实例，0 失败 0 跳过。实测环境为 JDK 17.0.17.10（由 `gradle/gradle-daemon-jvm.properties` 选定，launcher JVM 为 JDK 24），命令为 `test --rerun-tasks --no-build-cache`。

`build` 成功不等于测试跑过了。本工程不覆写 `~/.gradle/gradle.properties`，若其中开启了 `org.gradle.caching`，`:test` 会直接输出 `FROM-CACHE` 而一个测试都不执行，构建照样 `BUILD SUCCESSFUL`。验收以 `test --rerun-tasks` 为准，并核对 `build/test-results/test/` 下 XML 的数量与 `failures` / `errors` / `skipped` 计数。

协程栈恢复（`kotlinx.coroutines.stacktrace.recovery`）保持库默认值，工程未做任何覆盖。测试中的同一性断言用的是匿名 `Throwable` 子类、`AssertionError` 与 `CancellationException`——前两者无法被恢复机制克隆，后者本就被排除在恢复之外——因此在恢复开启时同样成立。

## 主要类型

| 类型 | 职责 |
| --- | --- |
| `TaskManager` | 全局单例，统一管理命名队列与便捷提交入口 |
| `TaskQueueScope` | 带独立生命周期的队列封装，按 `maxConcurrency` 自动选择队列类型 |
| `SerialTaskQueue` | 有序队列，支持 `pause` / `resume`，按优先级与入队序号排序 |
| `ConcurrentTaskQueue` | 并发队列，默认并发度 `DEFAULT_CONCURRENCY = 4`，提供 `awaitCompletion` |
| `Task` | 任务定义与构建链路，携带优先级、回调与惰性求值的 `name` |
| `TaskState` / `TaskPriority` | 状态机与优先级 |
| `EnqueueResult` | 入队结果四态：`Success` / `Rejected` / `Discarded` / `CallerExecuted` |
| `BackpressureConfig` / `RejectionPolicy` | 容量与拒绝策略，工厂方法 `unbounded` / `discard` / `discardOldest` / `throwException` / `callerRuns` / `block` |
| `QueueStateFlow` / `QueueExecutionContext` / `QueueLifecycle` | 状态发布、在途任务追踪与所有者取消回调 |

`SerialTaskQueue`、`ConcurrentTaskQueue` 与 `Task` 的 KDoc 里带有"相对旧实现的调整"小节（共 4 处），记录了设计取舍的原因，修改这三个类型的行为前请先阅读；其余类型没有该小节。

## 来源

源码迁出自 `Android-Desktop` 工程的 `core:tasks` 模块，包名由 `com.cn.desktop.core.tasks` 改为中立的 `com.cn.tasks`。迁出时做了三处解耦：

1. `core:common` 的 `Logger` 替换为本库的 `TaskLog` 接缝；
2. 移除 Hilt 限定符 `@ApplicationScope`，改为在文档中约定"宿主注入应用级作用域"；
3. `kotlinx-coroutines-core` 由原先经 `core:common` 传递改为直接 `api` 声明。

队列语义与源模块一致，可执行代码的差异只有上述第 1、2 两点：26 个测试文件中有 23 个在归一化包名后与源模块逐字节相同，主源中除 `Logger` → `TaskLog` 的调用替换与 `@ApplicationScope` 注解移除外无其他改动。另有三处不在解耦清单内、且不影响队列语义的调整：

- `TaskManager.kt` 与 `TaskQueueScope.kt` 的 KDoc 小节标题由"相对旧实现的调整"改为"作用域所有权"，正文重写为宿主中立表述，去掉了对 `com.cn.desktop.core.platform.di.CoroutinesModule` 的引用；
- `TaskManager` 未初始化时的报错文案由"`initialize(applicationScope)`，传入 `@ApplicationScope CoroutineScope`"改为"`initialize(scope)`，传入宿主的应用级 `CoroutineScope`"。这是唯一一处调用方可观测的非文档差异，因限定符已移除而必须改；
- `QueueDuplicateIdTest`、`QueueProgressCallbackTest`、`ConcurrentTaskQueueTest` 中等待生产者阻塞的轮询改为单次读取线程状态。原写法在循环条件与断言里各读一次 `thread.state`，而 `Condition.await` 允许伪唤醒，线程可能在两次读取之间离开 `WAITING`，产生 `expected:<WAITING> but was:<RUNNABLE>` 的间歇失败（30 轮全量运行中实测出现 1 次，单独运行该类不复现）。断言语义未变，仅消除读取竞态。

源工程中的 `core:tasks` 目前仍然保留：`Android-Desktop/settings.gradle.kts` 中 `include(":core:tasks")` 未被摘除，包名仍为 `com.cn.desktop.core.tasks`。
