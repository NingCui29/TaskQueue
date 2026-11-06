pluginManagement { repositories { google(); mavenCentral(); gradlePluginPortal() } }
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven {
            url = uri("../../build/maven-repository")
            content { includeGroup("com.cn.tasks") }
        }
        google(); mavenCentral()
    }
}
rootProject.name = "TaskQueueAarConsumer"
include(":app")
