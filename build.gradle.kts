import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `maven-publish`
}

group = "com.cn.tasks"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
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

publishing {
    publications {
        create<MavenPublication>("library") {
            from(components["java"])
        }
    }
}
