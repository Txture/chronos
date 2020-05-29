package org.chronos.chronograph.test.base;

import java.lang.annotation.*;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FailOnAllVerticesQuery {

}
