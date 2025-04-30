plugins {
    java
    alias(libs.plugins.kotlin.jvm)
    id("maven-publish")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "ChronoDB Exodus Backend"
        )
    }
}

val sourceJar by tasks.registering(Jar::class) {
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                artifact(sourceJar.get())

                groupId = project.group.toString()
                artifactId = project.name
                version = project.findProperty("mavenVersion") as String
            }
        }
    }
}

tasks.test {
    useJUnitPlatform {
        excludeTags("slow", "performance")
    }
}

dependencies {
    // Chronos project dependencies
    api(project(":org.chronos.common"))
    api(project(":org.chronos.chronodb.api"))
    testImplementation(project(":org.chronos.common.testing"))

    // Exodus
    implementation(libs.xodus.openapi)
    implementation(libs.xodus.environment)
    implementation(libs.apache.commons.io)
    implementation(libs.apache.commons.lang3)
    implementation(libs.apache.commons.configuration2)
    implementation(libs.guava)

    // Utilities
    implementation(libs.kryo)
    implementation(libs.xstream)
    implementation(libs.kotlin.logging.jvm)

    testImplementation(libs.bundles.testing)
}
