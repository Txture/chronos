plugins {
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
            "Implementation-Title" to "ChronoSphere"
        )
    }
}

tasks.test {
    useJUnit {
        excludeCategories("org.chronos.common.test.junit.categories.PerformanceTest")
    }
    filter {
        includeTestsMatching("org.chronos.chronosphere.test.cases.*")
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
                version = project.property("mavenVersion") as String
            }
        }

        repositories {
            maven {
                setUrl(project.property("s3Url") as String)
                credentials(AwsCredentials::class) {
                    accessKey = project.property("s3AccessKey") as? String
                    secretKey = project.property("s3SecretKey") as? String
                }
            }
        }

    }

}

val chronoDbApiTestSources = project(":org.chronos.chronodb.api").extensions
    .getByType<JavaPluginExtension>()
    .sourceSets
    .getByName("test")
    .output

val chronoGraphTestSources = project(":org.chronos.chronograph").extensions
    .getByType<JavaPluginExtension>()
    .sourceSets
    .getByName("test")
    .output

dependencies {
    api(project(":org.chronos.common"))
    api(project(":org.chronos.chronodb.api"))
    api(project(":org.chronos.chronograph"))
    testImplementation(project(":org.chronos.common.testing"))
    testImplementation(chronoDbApiTestSources)
    testImplementation(chronoGraphTestSources)
    testImplementation(project(":org.chronos.chronodb.exodus"))
    testImplementation(libs.apache.commons.io)

    api(libs.emf.ecore)
    api(libs.emf.common)
    api(libs.emf.xmi)

    api(libs.gremlin.core){
        exclude(group = "commons-io")
        exclude(group = "org.apache.commons", module = "commons-text")
        exclude(group = "org.apache.commons", module = "commons-lang3")
        exclude(group = "org.apache.commons", module = "commons-configuration2")
    }

    implementation(libs.guava)
    implementation(libs.apache.commons.lang3)
    implementation(libs.apache.commons.configuration2)


    testImplementation(libs.gremlin.test) {
        exclude(group = "commons-io")
        exclude(group = "org.apache.commons", module = "commons-text")
        exclude(group = "org.apache.commons", module = "commons-lang3")
        exclude(group = "org.apache.commons", module = "commons-configuration2")
    }

    testImplementation(libs.bundles.testing)
}
