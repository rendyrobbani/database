package com.rendyrobbani.database.mariadb;

import com.rendyrobbani.database.mariadb.anotation.Column;
import com.rendyrobbani.database.mariadb.anotation.PrimaryKey;
import com.rendyrobbani.database.mariadb.anotation.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Getter(AccessLevel.PROTECTED)
@Accessors(fluent = true)
@SuppressWarnings({"unchecked", "SqlNoDataSourceInspection", "SqlWithoutWhere"})
public abstract class DatabaseRepository<T> {

	private final Class<T> tableClass;

	private final String tableName;

	private final Connection connection;

	protected DatabaseRepository(Connection connection) {
		Type type = getClass().getGenericSuperclass();
		if (type instanceof ParameterizedType parameterizedType) this.tableClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
		else throw new IllegalArgumentException("Cannot create repository for class '" + getClass().getName() + "'");
		if (!this.tableClass.isAnnotationPresent(Table.class)) throw new IllegalArgumentException("Class '" + this.tableClass.getName() + "' is not annotated with @com.rendyrobbani.database.mariadb.anotation.Table");
		this.tableName = this.tableClass.getAnnotation(Table.class).name();
		this.connection = connection;
	}

	protected void setFieldValue(PreparedStatement statement, int index, Field field, Object value) throws Exception {
		if (value == null) statement.setNull(index, Types.NULL);
		else {
			Class<?> type = field.getType();
			if (type == String.class) statement.setString(index, (String) value);
			else if (type == Integer.class || type == int.class) statement.setInt(index, (Integer) value);
			else if (type == Long.class || type == long.class) statement.setLong(index, (Long) value);
			else if (type == Double.class || type == double.class) statement.setDouble(index, (Double) value);
			else if (type == Float.class || type == float.class) statement.setFloat(index, (Float) value);
			else if (type == Short.class || type == short.class) statement.setShort(index, (Short) value);
			else if (type == Byte.class || type == byte.class) statement.setByte(index, (Byte) value);
			else if (type == Boolean.class || type == boolean.class) statement.setBoolean(index, (Boolean) value);
			else if (type == java.math.BigDecimal.class) statement.setBigDecimal(index, (java.math.BigDecimal) value);
			else if (type == java.time.LocalDate.class) statement.setDate(index, java.sql.Date.valueOf((java.time.LocalDate) value));
			else if (type == java.time.LocalDateTime.class) statement.setTimestamp(index, java.sql.Timestamp.valueOf((java.time.LocalDateTime) value));
			else if (type.isEnum()) statement.setString(index, value.toString());
			else statement.setObject(index, value);
		}
	}

	private T toRow(ResultSet rs) {
		try {
			T row = tableClass.getConstructor().newInstance();
			for (Field field : DatabaseUtil.filterFieldOfColumns(tableClass)) {
				field.setAccessible(true);
				field.set(row, rs.getObject(field.getAnnotation(com.rendyrobbani.database.mariadb.anotation.Column.class).name(), field.getType()));
				field.setAccessible(false);
			}
			return row;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<T> findAll() {
		String sql = "select * from " + tableName;
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			try (ResultSet rs = statement.executeQuery()) {
				List<T> rows = new ArrayList<>();
				while (rs.next()) rows.add(toRow(rs));
				return rows;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<T> findBy(String columnName, Object value) {
		try {
			Field field = DatabaseUtil.filterFieldOfColumns(tableClass).stream().filter(f -> f.getAnnotation(Column.class).name().equals(columnName)).findFirst().orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' is not present in class '" + tableClass.getName() + "'"));
			String sql = "select * from " + tableName + " where " + columnName + " = ?";
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				setFieldValue(statement, 1, field, value);
				try (ResultSet rs = statement.executeQuery()) {
					List<T> rows = new ArrayList<>();
					while (rs.next()) rows.add(toRow(rs));
					return rows;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public T save(T row) {
		try {
			List<String> ins = new ArrayList<>();
			List<String> val = new ArrayList<>();
			List<String> upd = new ArrayList<>();

			List<Field> fields = DatabaseUtil.filterFieldOfColumns(tableClass);
			for (Field field : fields) {
				Field target = row.getClass().getDeclaredField(field.getName());
				target.setAccessible(true);
				Object object = target.get(row);
				boolean isAutoIncrement = field.isAnnotationPresent(PrimaryKey.class) && field.getAnnotation(PrimaryKey.class).autoIncrement() && (field.getType() == Long.class || field.getType() == long.class);
				if (isAutoIncrement && object == null) continue;

				String c = field.getAnnotation(Column.class).name();
				String v = "?";

				ins.add(c);
				val.add(v);

				boolean isPrimaryKey = field.isAnnotationPresent(PrimaryKey.class) || isAutoIncrement;
				if (isPrimaryKey) continue;

				upd.add(c + " = " + v);
			}

			List<String> sql = new ArrayList<>();
			if (!ins.isEmpty()) sql.add("insert into " + tableName + " (" + String.join(", ", ins) + ")");
			if (!val.isEmpty()) sql.add("values (" + String.join(", ", val) + ")");
			if (!upd.isEmpty()) sql.add("on duplicate key update " + String.join(", ", upd));
			try (PreparedStatement statement = connection.prepareStatement(String.join(System.lineSeparator(), sql))) {
				int index = 1;
				for (int i = 0; i < 2; i++) {
					for (Field field : fields) {
						Field target = row.getClass().getDeclaredField(field.getName());
						target.setAccessible(true);
						Object object = target.get(row);
						boolean isAutoIncrement = field.isAnnotationPresent(PrimaryKey.class) && field.getAnnotation(PrimaryKey.class).autoIncrement() && (field.getType() == Long.class || field.getType() == long.class);
						if (isAutoIncrement && object == null) continue;

						boolean isPrimaryKey = field.isAnnotationPresent(PrimaryKey.class) || isAutoIncrement;
						if (i == 0 || !isPrimaryKey) setFieldValue(statement, index++, field, object);
					}
				}
				statement.execute();
				return row;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void deleteAll() {
		String sql = "delete from " + tableName;
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.executeUpdate();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void deleteBy(String columnName, Object value) {
		try {
			Field field = DatabaseUtil.filterFieldOfColumns(tableClass).stream().filter(f -> f.getAnnotation(Column.class).name().equals(columnName)).findFirst().orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' is not present in class '" + tableClass.getName() + "'"));
			String sql = "delete from " + tableName + " where " + columnName + " = ?";
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				setFieldValue(statement, 1, field, value);
				statement.execute();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}