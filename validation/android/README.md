# Android AAR 验收工程

独立 Android 应用从根项目生成的 Maven 仓库解析 `com.cn.tasks:TaskQueue-android:0.1.0`，验证 AAR 接入、传递依赖和公开队列 API。此工程不包含在根项目的常规 `build` 中。

需要 JDK 17、Android SDK（包含 API 35）及已连接、允许调试的 Android 设备。可通过 `ANDROID_HOME` 指定 SDK，或在本目录创建不提交的 `local.properties`。应用最低要求 API 26；现有设备验收记录覆盖 API 32。

在仓库根目录先生成发布文件和本地 Maven 仓库，再编译验收 APK：

```powershell
.\gradlew.bat releaseBundle --no-build-cache --console=plain
.\gradlew.bat -p validation/android :app:assembleDebug :app:assembleDebugAndroidTest --no-build-cache --console=plain
```

连接设备后，使用实际设备序列号替换下方的 `<设备序列号>`，在仓库根目录运行：

```powershell
adb -s <设备序列号> install -r -t validation/android/app/build/outputs/apk/debug/app-debug.apk
adb -s <设备序列号> install -r -t validation/android/app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk
adb -s <设备序列号> shell am instrument -w -r -e class com.cn.tasks.validation.RealDispatcherDeviceTest com.cn.tasks.validation.aar.test/androidx.test.runner.AndroidJUnitRunner
```

Linux/macOS 使用 `./gradlew` 替代 `.\gradlew.bat`。测试安装独立验收应用 `com.cn.tasks.validation.aar` 和测试 APK，不替换业务应用包。结束后可通过 `adb uninstall com.cn.tasks.validation.aar.test` 和 `adb uninstall com.cn.tasks.validation.aar` 清理。

`RealDispatcherDeviceTest` 包含 10 个 APK 内执行的测试，覆盖串行/并发队列在真实 `Main.immediate`、`Default`、`IO` 调度器上的线程与结果、等待任务取消/淘汰、取消清理、回调重入和日志异常隔离。2026-11-06 已使用本工程构建的 APK 在 API 32 设备执行，10 个测试全部通过。编译 APK 本身不等于设备测试通过。

测试仅使用公开 API，不配置 friend paths。构建产物位于 `app/build/outputs/`，instrumentation 成功输出应包含 `OK (10 tests)`。

也可运行 `:app:connectedDebugAndroidTest` 生成 HTML 报告。但 Windows 下的部分 AGP 版本会将无线设备序列号中的冒号写入 profiling 文件名，导致设备测试尚未启动就失败；遇到该情况请使用上述直接 instrumentation 命令。该文件名错误不属于队列测试失败。
