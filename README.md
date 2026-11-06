# TaskQueue

基于 Kotlin Coroutines 的任务队列库，支持串行与并发执行、优先级调度、进度通知、超时和背压控制。核心不依赖 Android 或依赖注入框架，同时提供 JVM JAR 和 Android AAR。

[![持续集成](https://github.com/NingCui29/TaskQueue/actions/workflows/ci.yml/badge.svg)](https://github.com/NingCui29/TaskQueue/actions/workflows/ci.yml)
[![版本发布](https://img.shields.io/github/v/release/NingCui29/TaskQueue)](https://github.com/NingCui29/TaskQueue/releases)
[![MIT 许可证](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## 功能

- 串行队列：按优先级调度，同优先级保持 FIFO，支持暂停与恢复。
- 并发队列：限制常规执行任务的并发数，支持等待队列排空。
- 背压策略：拒绝并抛错、丢弃新任务、淘汰旧任务、调用者执行、阻塞等待。
- 任务生命周期：状态、进度、成功、错误、完成和超时回调。
- 取消与关闭：跟踪在途任务和清理过程，不取消宿主提供的共享作用域。
- 可替换日志输出端，普通日志异常不改变任务结果。

架构和业务流程见 [架构与流程图](docs/architecture.md)。

## 依赖接入

当前版本：**0.1.0**。在项目的 `settings.gradle.kts` 中添加仓库：

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://raw.githubusercontent.com/NingCui29/TaskQueue/maven") {
            content { includeGroup("com.cn.tasks") }
        }
    }
}
```

Android 项目使用 AAR：

```kotlin
dependencies {
    implementation("com.cn.tasks:TaskQueue-android:0.1.0")
}
```

JVM 项目使用 JAR：

```kotlin
dependencies {
    implementation("com.cn.tasks:TaskQueue:0.1.0")
}
```

**两种坐标二选一**，不能同时引入；它们包含相同的核心类。通过 Maven 引用会自动解析 Kotlin 标准库和协程依赖。

| 环境 | 要求 |
| --- | --- |
| JVM / 字节码 | Java 17 |
| Kotlin 构建版本 | 2.1.20 |
| kotlinx-coroutines | 1.8.1 |
| AAR 声明的最低 Android 版本 | API 26 |
| 已完成真机验证 | Android 12 / API 32 / arm64 |

API 26 是 AAR 的接入下限，已完成的真机验证范围为 API 32。使用 `Dispatchers.Main` 的 Android 宿主还需要 `kotlinx-coroutines-android`。

也可从 [GitHub Releases](https://github.com/NingCui29/TaskQueue/releases) 下载 AAR、JAR、源码包和 Maven 仓库压缩包。直接引用本地 AAR 时需自行声明依赖：

```kotlin
dependencies {
    implementation(files("libs/TaskQueue-android-0.1.0.aar"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib:2.1.20")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
}
```

### 可选：GitHub Packages

GitHub Packages 是第二条分发渠道，上面的匿名 Maven 仓库继续保留。Packages 的公开 Maven 包下载也需要认证，普通消费者需要自己的 **个人访问令牌（classic）**，至少具有 `read:packages` 权限；不支持匿名下载。规则见 [GitHub 官方文档](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry#authenticating-to-github-packages)。

在用户级 `~/.gradle/gradle.properties` 配置 `gpr.user`（GitHub 用户名）和 `gpr.key`（PAT），或使用环境变量 `GITHUB_ACTOR`、`GITHUB_TOKEN`。不要将凭据写入项目或提交到 Git。选择此渠道时，将匿名仓库配置替换为：

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://maven.pkg.github.com/ningcui29/taskqueue") {
            content { includeGroup("com.cn.tasks") }
            credentials {
                username = providers.gradleProperty("gpr.user")
                    .orElse(providers.environmentVariable("GITHUB_ACTOR")).orNull
                password = providers.gradleProperty("gpr.key")
                    .orElse(providers.environmentVariable("GITHUB_TOKEN")).orNull
            }
        }
    }
}
```

Packages 使用新的全小写坐标，因为该服务不接受大写 artifactId。按目标平台只选择一行：

```kotlin
dependencies {
    implementation("com.cn.tasks:taskqueue-android:0.1.0") // Android AAR
    // implementation("com.cn.tasks:taskqueue:0.1.0")     // JVM JAR
}
```

**两种渠道不要混用**：匿名 Maven 渠道使用 `TaskQueue` / `TaskQueue-android`，Packages 渠道使用 `taskqueue` / `taskqueue-android`；这些坐标包含相同的类，同时引入会产生重复类。原有大写坐标保持不变。Packages 是否已发布以仓库的 [Packages 页面](https://github.com/NingCui29/TaskQueue/packages) 和发布工作流结果为准。

## 快速开始

由宿主提供并管理作用域。以下示例创建应用级作用域，在宿主退出时由宿主取消：

```kotlin
import com.cn.tasks.*
import kotlinx.coroutines.*

val applicationScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
val queue = ConcurrentTaskQueue(
    scope = applicationScope,
    maxConcurrency = 4,
    dispatcher = Dispatchers.IO,
    backpressureConfig = BackpressureConfig.discardOldest(64),
)

val task = queue.enqueue(
    priority = TaskPriority.HIGH,
    block = { progress ->
        progress.report(0.5f, "正在处理")
        "处理结果"
    },
    onSuccess = { result -> println(result) },
    onStateChange = { state -> println(state) },
)

// 在协程中等待所有已接纳任务及清理退出，然后关闭队列。
// applicationScope 仍由宿主负责管理。
applicationScope.launch {
    queue.awaitCompletion()
    queue.shutdown()
}
```

回调不会自动切换到主线程。等待状态通知通常在提交或调度线程执行，进度回调在报告线程执行，执行结果回调在任务执行上下文中执行。更新 Android UI 时应由宿主切换到主线程。

### 串行队列与任务配置

```kotlin
import kotlin.time.Duration.Companion.seconds

val serial = SerialTaskQueue(applicationScope, dispatcher = Dispatchers.IO)
val work = task<String> {
    name = "LoadData"
    priority = TaskPriority.HIGH
    timeout = 30.seconds
    block { progress ->
        progress.report(0.5f)
        "result"
    }
    onSuccess { println(it) }
    onError { println(it.message) }
    onComplete { println("任务结束") }
}
serial.enqueue(work)
```

### 管理命名队列

```kotlin
TaskManager.initialize(applicationScope)
TaskManager.createSerialQueue("sync", dispatcher = Dispatchers.IO)
TaskManager.submitSerial(queueName = "sync", block = { _ -> "synced" })
```

`TaskManager` 必须先初始化。提交时指定的队列不存在，会回退到默认队列。需要组件级生命周期时，可使用 `TaskQueueScope.serial(applicationScope, "sync")` 或 `TaskQueueScope.concurrent(...)`；其 `shutdown()` 只关闭自身队列。

## 生命周期与取消

```text
Pending → Running → Completed（成功或失败）
   └────────┴────→ Cancelled
```

已接纳任务结束时，`onComplete` 每轮通知一次：等待中取消、淘汰、调度交接取消和协程启动前取消也会通知。执行中取消会等待任务体的清理（包括 `NonCancellable`）退出后，再通知完成。

状态回调抛出取消异常或致命错误时，仍尝试完成通知，并保留失败优先级。普通回调异常会记录日志，不替换任务业务结果。

| 操作 | 行为 |
| --- | --- |
| `Task.cancel()` | 仅设置取消标记，不直接中断运行中的任务或触发完成通知 |
| 串行 `cancel(id)` | 取消等待中或尚未交给执行协程的任务，不影响已交接执行的任务 |
| 并发 `cancel(id)` | 可取消等待中和执行中的任务 |
| `cancelAll()` | 取消当前任务集合，队列可继续使用 |
| `shutdown()` | 永久停止接收，取消现存任务，等待在途清理收敛 |
| 串行 `pause()` | 取消当前工作循环的任务并停止后续出队；被取消任务不会自动重跑 |
| 串行 `resume()` | 继续处理保留的等待任务 |
| 串行 `clear()` | 静默移除等待任务，状态保留为 Pending，不触发取消或完成通知 |

容量拒绝和关闭拒绝属于未接纳，不触发 `onComplete`，应检查 `EnqueueResult`。同一 `Task` 实例只能有一个在途提交；复用须等待上一轮执行及回调全部退出，不能跨队列同时提交。取消标记不会因重新提交而清除。

队列 `isClosed` 表示已停止接收，`isTerminated` 表示在途任务和调度循环均已退出。`shutdown()` 本身不是阻塞等待方法；并发队列可在其后调用挂起版 `awaitCompletion()` 等待清理。

## 背压策略

队列构造器默认无界；`maxCapacity` 仅限制等待任务，不含执行中的任务。

| 策略 | 满容量时的行为 |
| --- | --- |
| `THROW_EXCEPTION` | `enqueueWithResult` 返回拒绝；便捷 `enqueue` 抛出 `TaskQueueFullException` |
| `DISCARD` | 丢弃新任务，保留旧任务 |
| `DISCARD_OLDEST` | 淘汰优先级最低、同优先级最早入队的等待任务，接纳新任务 |
| `CALLER_RUNS` | 阻塞调用者并就地执行，可能超过常规并发上限，也不受串行暂停控制 |
| `BLOCK` | 阻塞线程直到有空位；满容量的同队列执行或通知重入会被拒绝 |

`CALLER_RUNS` 和 `BLOCK` 不应在主线程使用。`BLOCK` 也不能占住队列推进所需的调度线程。等待位次会随队列变更全量排序刷新，大量积压场景应配置合理容量。

```kotlin
when (val result = queue.enqueueWithResult(Task(block = { "value" }))) {
    is EnqueueResult.Success -> println("已入队")
    is EnqueueResult.Discarded -> println("已接纳新任务，淘汰 ${result.discardedTask.id}")
    is EnqueueResult.CallerExecuted -> println("就地执行成功：${result.success}")
    is EnqueueResult.Rejected -> println("拒绝：${result.cause} / ${result.reason}")
}
```

## 日志

日志默认关闭，输出端由宿主注入：

```kotlin
import com.cn.tasks.log.TaskLog

TaskLog.enabled = true
TaskLog.printer = TaskLog.Printer { priority, tag, message, throwable ->
    println("[$priority][$tag] $message")
    throwable?.printStackTrace()
}
```

日志输出器的普通异常被隔离；取消异常和致命错误继续传播。优先级常量与 `android.util.Log` 对齐，Android 宿主可自行转接。

## 构建与验证

```shell
./gradlew build
./gradlew test --rerun-tasks --no-build-cache
./gradlew releaseBundle
```

Windows 使用 `gradlew.bat`。构建守护进程固定使用 JDK 17；核心构建与 AAR 打包均不要求安装 Android SDK。发布产物位于 `build/release/`，文件 Maven 仓库位于 `build/maven-repository/`。

可复现的 Android 专项工程见 [真机验证说明](validation/android/README.md)，该工程需要 Android SDK 和可用设备。

2026-11-06 验证结果：

- JVM：28 个测试类、132 个测试通过，0 失败、0 跳过。
- 真机：Android 12 / API 32 / arm64，132 个既有回归测试及 10 个真实 Main、Default、IO 调度器专项通过，共 142 个测试。
- AAR 接入：独立 Android 项目通过 Maven 解析依赖，Debug 与 R8 Release 构建通过；AAR 的 10 个真实调度器专项真机复测通过。
- 验证重点：取消完成通知、清理时序、回调重入、日志异常隔离。

以上不包含宿主业务端到端、其他 Android 版本、长时间压力或性能验收。判断测试是否实际执行时，应查看 Gradle 输出和测试 XML；`FROM-CACHE` 不代表本次重新运行了测试。

## 版本与发布

- `main`：日常开发分支。
- `release/0.1.0`：0.1.0 版本分支。
- `v0.1.0`：固定版本 Tag，对应 GitHub Release。
- `maven`：依赖仓库分支，仅存放发布产物及依赖元数据。

发布包包含 JAR、AAR、源码 JAR、POM、Maven 仓库压缩包和 `SHA256SUMS.txt`。依赖包未发布到 Maven Central；请使用本文提供的仓库地址。下载后可使用 `sha256sum -c SHA256SUMS.txt` 核验文件。

可选的 [GitHub Packages 发布工作流](.github/workflows/packages.yml) 支持手动运行和 GitHub Release 发布事件。它先构建并测试，再以 `-PgithubPackages=true` 发布小写坐标，最后通过认证下载远端 POM、JAR/AAR、源码及 JVM 模块元数据，核对坐标与本地产物 SHA-256。未设置该参数时，`releaseBundle` 和文件 Maven 仓库仍使用原有大写坐标。

变更记录见 [CHANGELOG.md](CHANGELOG.md)。许可证：[MIT](LICENSE)。
