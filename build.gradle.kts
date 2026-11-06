import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.security.MessageDigest

plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `maven-publish`
}

group = "com.cn.tasks"
version = "0.1.0"

// GitHub Packages 的 artifactId 必须小写；默认发布坐标保持现有兼容性。
val githubPackagesMode = providers.gradleProperty("githubPackages")
    .map { it.toBooleanStrict() }
    .orElse(false)
    .get()

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        // 接口默认方法直接生成到字节码，减少 DefaultImpls 桥接类；与源工程一致
        freeCompilerArgs.add("-Xjvm-default=all")
    }
}

dependencies {
    // CoroutineScope / CoroutineDispatcher / Flow 都出现在公开签名上，必须以 api 暴露。
    // 源工程里这条依赖只经 core:common 传递而来，迁出后必须显式声明。
    api(libs.kotlinx.coroutines.core)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

// 核心不依赖平台，直接将同一份 JVM JAR 封装为 AAR，不重复编译。
val androidAar by tasks.registering(Zip::class) {
    archiveExtension.set("aar")
    archiveFileName.set("TaskQueue-android-${project.version}.aar")
    destinationDirectory.set(layout.buildDirectory.dir("libs"))
    from(tasks.jar.flatMap { it.archiveFile }) { rename { "classes.jar" } }
    from("packaging/android")
    from("LICENSE") { into("META-INF") }
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.withType<Jar>().configureEach {
    from("LICENSE") { into("META-INF") }
}

publishing {
    publications {
        create<MavenPublication>("library") {
            from(components["java"])
            if (githubPackagesMode) artifactId = "taskqueue"
        }
        create<MavenPublication>("android") {
            from(components["java"])
            artifactId = if (githubPackagesMode) "taskqueue-android" else "TaskQueue-android"
            setArtifacts(listOf(androidAar, tasks.named("sourcesJar")))
        }
        withType<MavenPublication>().configureEach {
            pom {
                name.set(artifactId)
                description.set("基于 Kotlin 协程的任务队列，支持优先级调度、背压控制和生命周期回调")
                url.set("https://github.com/NingCui29/TaskQueue")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/license/mit")
                    }
                }
                developers {
                    developer { id.set("NingCui29"); name.set("Cuining") }
                }
                scm {
                    connection.set("scm:git:https://github.com/NingCui29/TaskQueue.git")
                    developerConnection.set("scm:git:ssh://git@github.com/NingCui29/TaskQueue.git")
                    url.set("https://github.com/NingCui29/TaskQueue")
                }
            }
        }
    }
    repositories {
        maven {
            name = "release"
            url = layout.buildDirectory.dir("maven-repository").get().asFile.toURI()
        }
        if (githubPackagesMode) {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/ningcui29/taskqueue")
                credentials {
                    username = providers.gradleProperty("gpr.user")
                        .orElse(providers.environmentVariable("GITHUB_ACTOR")).orNull
                    password = providers.gradleProperty("gpr.key")
                        .orElse(providers.environmentVariable("GITHUB_TOKEN")).orNull
                }
            }
        }
    }
}

// Android 发布物是 AAR，不使用 Java 组件生成的 JAR 变体元数据。
tasks.withType<GenerateModuleMetadata>().configureEach {
    if (name == "generateMetadataFileForAndroidPublication") enabled = false
}

val mavenRepositoryZip by tasks.registering(Zip::class) {
    dependsOn("publishAllPublicationsToReleaseRepository")
    archiveFileName.set("TaskQueue-${project.version}-maven-repository.zip")
    destinationDirectory.set(layout.buildDirectory.dir("release"))
    from(layout.buildDirectory.dir("maven-repository"))
}

val stageRelease by tasks.registering(Copy::class) {
    dependsOn("publishAllPublicationsToReleaseRepository")
    into(layout.buildDirectory.dir("release"))
    from(tasks.jar.flatMap { it.archiveFile })
    from(tasks.named<Jar>("sourcesJar").flatMap { it.archiveFile })
    from(androidAar.flatMap { it.archiveFile })
    from(layout.buildDirectory.file("publications/library/pom-default.xml")) {
        rename { "TaskQueue-${project.version}.pom" }
    }
    from(layout.buildDirectory.file("publications/android/pom-default.xml")) {
        rename { "TaskQueue-android-${project.version}.pom" }
    }
    from("LICENSE", "CHANGELOG.md")
}

tasks.register("releaseBundle") {
    group = "distribution"
    description = "构建 JAR、AAR、源码包、Maven 仓库和 SHA-256 校验文件。"
    dependsOn(stageRelease, mavenRepositoryZip)
    doLast {
        val directory = layout.buildDirectory.dir("release").get().asFile
        val entries = directory.listFiles()!!.filter { it.isFile && it.name != "SHA256SUMS.txt" }.sortedBy { it.name }
        directory.resolve("SHA256SUMS.txt").writeText(entries.joinToString("\n", postfix = "\n") { file ->
            val hash = MessageDigest.getInstance("SHA-256").digest(file.readBytes()).joinToString("") { "%02x".format(it) }
            "$hash  ${file.name}"
        })
    }
}
