plugins {
    kotlin("jvm") version "2.2.21"
    application
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("lps.MainKt")
}
