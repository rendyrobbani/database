package com.rendyrobbani.database.mariadb.anotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ForeignKey {

	String[] columns();

	Class<?> referenceTable();

	String[] referenceColumn();

}