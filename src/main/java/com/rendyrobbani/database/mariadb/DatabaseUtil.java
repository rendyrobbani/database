package com.rendyrobbani.database.mariadb;

import com.rendyrobbani.database.mariadb.anotation.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DatabaseUtil {

	private static String type(Field field) {
		Column column = field.getAnnotation(Column.class);
		if (field.getType() == Long.class || field.getType() == long.class) return "bigint";
		if (field.getType() == Integer.class || field.getType() == int.class) return "int";
		if (field.getType() == Short.class || field.getType() == short.class) return "smallint";
		if (field.getType() == Byte.class || field.getType() == byte.class) return "tinyint";
		if (field.getType() == Boolean.class || field.getType() == boolean.class) return "bit";
		if (field.getType() == Double.class || field.getType() == double.class) return "double";
		if (field.getType() == Float.class || field.getType() == float.class) return "float";
		if (field.getType() == Character.class || field.getType() == char.class) return "char(" + column.length() + ")";
		if (field.getType() == String.class) return "varchar(" + column.length() + ")";
		if (field.getType() == LocalDate.class) return "date";
		if (field.getType() == LocalDateTime.class) return "datetime";
		if (field.getType() == BigDecimal.class) return "decimal(38, 2)";
		throw new IllegalArgumentException("Type '" + field.getType().getName() + "' is not supported");
	}

	private static String constraintName(String prefix, String tableName, int index) {
		return prefix + "_" + tableName + "_" + String.format("%02d", index);
	}

	private static List<String> rowsOfPrimaryKeys(Class<?> tableClass) {
		List<String> columns = filterFieldOfColumns(tableClass).stream().filter(field -> field.isAnnotationPresent(PrimaryKey.class)).map(field -> field.getAnnotation(Column.class).name()).toList();
		return columns.isEmpty() ? List.of() : List.of("primary key (" + String.join(", ", columns) + ")");
	}

	private static List<String> rowsOfForeignKeys(Class<?> tableClass) {
		if (!tableClass.isAnnotationPresent(ForeignKeys.class)) return List.of();
		Map<String, Field> from = filterFieldOfColumns(tableClass).stream().collect(Collectors.toMap(field -> field.getAnnotation(Column.class).name(), Function.identity()));
		List<String> foreignKeys = new ArrayList<>();
		for (ForeignKey foreignKey : tableClass.getAnnotation(ForeignKeys.class).value()) {
			List<String> fromColumns = new ArrayList<>();
			for (String columnName : foreignKey.columns()) {
				if (!from.containsKey(columnName)) throw new IllegalArgumentException("Column '" + columnName + "' is not present in class '" + tableClass.getName() + "'");
				fromColumns.add(columnName);
			}

			if (!foreignKey.referenceTable().isAnnotationPresent(Table.class)) throw new IllegalArgumentException("Class '" + foreignKey.referenceTable().getName() + "' is not annotated with @com.rendyrobbani.database.mariadb.anotation.Table");
			String intoTable = foreignKey.referenceTable().getAnnotation(Table.class).name();

			Map<String, Field> into = filterFieldOfColumns(foreignKey.referenceTable()).stream().collect(Collectors.toMap(field -> field.getAnnotation(Column.class).name(), Function.identity()));

			List<String> intoColumns = new ArrayList<>();
			for (String columnName : foreignKey.referenceColumns()) {
				if (!into.containsKey(columnName)) throw new IllegalArgumentException("Column '" + columnName + "' is not present in class '" + foreignKey.referenceTable().getName() + "'");
				intoColumns.add(columnName);
			}

			String name = constraintName("fk", tableClass.getAnnotation(Table.class).name(), foreignKeys.size() + 1);
			String fColumn = "(" + String.join(", ", fromColumns) + ")";
			String tColumn = "(" + String.join(", ", intoColumns) + ")";
			foreignKeys.add("\t" + String.join(" ", "constraint", name, "foreign key", fColumn, "references", intoTable, tColumn));
		}
		return foreignKeys;
	}

	private static List<String> rowsOfUniqueKeys(Class<?> tableClass) {
		if (!tableClass.isAnnotationPresent(UniqueKeys.class)) return List.of();
		Map<String, Field> columns = filterFieldOfColumns(tableClass).stream().collect(Collectors.toMap(field -> field.getAnnotation(Column.class).name(), Function.identity()));
		List<String> uniqueKeys = new ArrayList<>();
		for (UniqueKey uniqueKey : tableClass.getAnnotation(UniqueKeys.class).value()) {
			List<String> columnsList = new ArrayList<>();
			for (String columnName : uniqueKey.columns()) {
				if (!columns.containsKey(columnName)) throw new IllegalArgumentException("Column '" + columnName + "' is not present in class '" + tableClass.getName() + "'");
				columnsList.add(columnName);
			}
			String name = constraintName("uk", tableClass.getAnnotation(Table.class).name(), uniqueKeys.size() + 1);
			uniqueKeys.add("\t" + String.join(" ", "constraint", name, "unique", "(" + String.join(", ", columnsList) + ")"));
		}
		return uniqueKeys;
	}

	private static List<String> rowsOfChecks(Class<?> tableClass) {
		if (!tableClass.isAnnotationPresent(Checks.class)) return List.of();
		List<String> checks = new ArrayList<>();
		for (Check check : tableClass.getAnnotation(Checks.class).value()) {
			String name = constraintName("ck", tableClass.getAnnotation(Table.class).name(), checks.size() + 1);
			checks.add("\t" + String.join(" ", "constraint", name, "check", "(" + check.expression() + ")"));
		}
		return checks;
	}

	private static List<String> rowsOfColumns(Class<?> tableClass) {
		List<Field> fields = filterFieldOfColumns(tableClass);

		int maxColName = 0;
		int maxColType = 0;
		for (Field field : fields) {
			maxColName = Math.max(maxColName, field.getAnnotation(Column.class).name().length());
			maxColType = Math.max(maxColType, type(field).length());
		}

		List<String> rowsOfColumns = new ArrayList<>();
		for (Field field : fields) {
			Column column = field.getAnnotation(Column.class);
			String name = column.name();
			String type = type(field);

			boolean isAutoIncrement = field.isAnnotationPresent(PrimaryKey.class) && field.getAnnotation(PrimaryKey.class).autoIncrement() && (field.getType() == Long.class || field.getType() == long.class);
			boolean isPrimaryKey = field.isAnnotationPresent(PrimaryKey.class) || isAutoIncrement;
			boolean isNullable = column.nullable() && !isPrimaryKey;

			List<String> row = new ArrayList<>();
			row.add("\t" + name);
			row.add(" ".repeat(maxColName - name.length()));
			row.add(type);
			row.add(" ".repeat(maxColType - type.length()));
			row.add(isNullable ? "null" : "not null");
			if (isAutoIncrement) row.add("auto_increment");
			rowsOfColumns.add(String.join(" ", row));
		}
		return rowsOfColumns;
	}

	public static List<Field> filterFieldOfColumns(Class<?> tableClass) {
		if (!tableClass.isAnnotationPresent(Table.class)) throw new IllegalArgumentException("Class '" + tableClass.getName() + "' is not annotated with @com.rendyrobbani.database.mariadb.anotation.Table");
		return Arrays.stream(tableClass.getDeclaredFields()).filter(field -> field.isAnnotationPresent(Column.class)).toList();
	}

	public static String DDLOfCreateTable(Class<?> tableClass, boolean useOrReplace) {
		List<String> rowsOfPrimaryKeys = rowsOfPrimaryKeys(tableClass);
		List<String> rowsOfForeignKeys = rowsOfForeignKeys(tableClass);
		List<String> rowsOfUniqueKeys = rowsOfUniqueKeys(tableClass);
		List<String> rowsOfChecks = rowsOfChecks(tableClass);
		List<String> rowsOfColumns = rowsOfColumns(tableClass);

		List<String> rowsOfDDL = new ArrayList<>();
		Table table = tableClass.getAnnotation(Table.class);
		rowsOfDDL.add("create " + (useOrReplace ? "or replace " : "") + "table " + table.name() + " (");
		for (int i = 0; i < rowsOfColumns.size(); i++) {
			boolean endsWithComma = i < rowsOfColumns.size() - 1;
			endsWithComma = endsWithComma || !rowsOfForeignKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfPrimaryKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfUniqueKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfChecks.isEmpty();
			rowsOfDDL.add(rowsOfColumns.get(i) + (endsWithComma ? "," : ""));
		}
		for (int i = 0; i < rowsOfChecks.size(); i++) {
			boolean endsWithComma = i < rowsOfChecks.size() - 1;
			endsWithComma = endsWithComma || !rowsOfForeignKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfPrimaryKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfUniqueKeys.isEmpty();
			rowsOfDDL.add(rowsOfChecks.get(i) + (endsWithComma ? "," : ""));
		}
		for (int i = 0; i < rowsOfForeignKeys.size(); i++) {
			boolean endsWithComma = i < rowsOfForeignKeys.size() - 1;
			endsWithComma = endsWithComma || !rowsOfPrimaryKeys.isEmpty();
			endsWithComma = endsWithComma || !rowsOfUniqueKeys.isEmpty();
			rowsOfDDL.add(rowsOfForeignKeys.get(i) + (endsWithComma ? "," : ""));
		}
		for (int i = 0; i < rowsOfUniqueKeys.size(); i++) {
			boolean endsWithComma = i < rowsOfUniqueKeys.size() - 1;
			endsWithComma = endsWithComma || !rowsOfPrimaryKeys.isEmpty();
			rowsOfDDL.add(rowsOfUniqueKeys.get(i) + (endsWithComma ? "," : ""));
		}
		for (String row : rowsOfPrimaryKeys) rowsOfDDL.add("\t" + row);
		rowsOfDDL.add(") engine = " + table.engine());
		rowsOfDDL.add("  charset = " + table.charset());
		rowsOfDDL.add("  collate = " + table.collate() + ";");
		return String.join(System.lineSeparator(), rowsOfDDL);
	}

	public static String DDLOfCreateTable(Class<?> tableClass) {
		return DDLOfCreateTable(tableClass, true);
	}

}