package com.rendyrobbani.database.mariadb.anotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {

	String name();

	String engine() default "innodb";

	String charset() default "utf8mb4";

	String collate() default "utf8mb4_unicode_ci";

}