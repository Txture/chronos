plugins {
    alias(libs.plugins.kotlin.jvm)
    id("maven-publish")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    sourceSets {
        test {

        }
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "ChronoDB API"
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

dependencies {
    // chronos project dependencies
    api(project(":org.chronos.common"))
    testImplementation(project(":org.chronos.common.testing"))
    testImplementation(project(":org.chronos.chronodb.exodus"))

    // Utilities
    implementation(libs.kryo)
    implementation(libs.xstream)
    implementation(libs.apache.commons.lang3)
    implementation(libs.apache.commons.io)
    implementation(libs.apache.commons.configuration2)
    implementation(libs.guava)
    implementation(libs.slf4j.api)


    testImplementation(libs.bundles.testing)
}
