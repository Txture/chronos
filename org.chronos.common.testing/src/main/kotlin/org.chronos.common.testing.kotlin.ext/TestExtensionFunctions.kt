package org.chronos.common.testing.kotlin.ext

import org.hamcrest.Matcher
import org.hamcrest.Matchers.*

import org.hamcrest.MatcherAssert.assertThat


infix fun <T> T?.shouldBe(other: T) {
    assertThat(this, `is`(other))
}

fun <T> T?.shouldBeNull(){
    assertThat(this, `is`(nullValue()))
}

@Suppress("UNCHECKED_CAST")
infix fun <T> T?.shouldBe(matcher: Matcher<in T>){
    assertThat(this, matcher as Matcher<in T?>?)
}


infix fun <T> T?.shouldNotBe(other: T?){
    assertThat(this, not(`is`(other)))
}

@Suppress("UNCHECKED_CAST")
infix fun <T> T?.shouldNotBe(matcher: Matcher<in T>){
    assertThat(this, not(matcher as Matcher<in T?>?))
}

fun <T> T?.shouldNotBeNull(){
    assertThat(this, `is`(notNullValue()))
}


infix fun <T> T.should(matcher: Matcher<in T>){
    assertThat(this, matcher)
}

fun <T> be(matcher: Matcher<in T>) = `is`(matcher)

fun <T> be(other: T) = `is`(other)

fun beNull() = `is`(nullValue())

fun notBeNull() = `is`(notNullValue())
