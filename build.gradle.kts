import org.gradle.kotlin.dsl.withType
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.versions)
    id("idea")
}

allprojects {

    repositories {
        mavenCentral()
    }

}


extra["mavenVersion"] = project.version

allprojects {
    group = "org.chronos"
    version = "1.3.29-SNAPSHOT"

    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_21)
        }
    }
}
