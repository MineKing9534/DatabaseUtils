package de.mineking.databaseutils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberStrategy;
import de.mineking.javautils.ID;
import de.mineking.databaseutils.type.DataType;
import de.mineking.databaseutils.type.PostgresType;
import de.mineking.javautils.reflection.ReflectionUtils;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.statement.StatementContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public interface TypeMapper<T, R> {
	boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f);

	@NotNull
	DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f);

	@NotNull
	default Argument createArgument(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable T value) {
		return new Argument() {
			@Override
			public void apply(int position, PreparedStatement statement, StatementContext ctx) throws SQLException {
				statement.setObject(position, value);
			}

			@Override
			public String toString() {
				return Objects.toString(value);
			}
		};
	}

	@NotNull
	Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable R value);

	@Nullable
	@SuppressWarnings("unchecked")
	default T format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable R value) {
		return (T) value;
	}

	@Nullable
	T extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException;

	@SuppressWarnings("unchecked")
	@Nullable
	default R parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable T value) {
		return (R) value;
	}

	TypeMapper<Integer, Integer> SERIAL = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return f.getAnnotation(Column.class).autoincrement() && (type.equals(int.class) || type.equals(long.class));
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(int.class) ? PostgresType.SERIAL : PostgresType.BIG_SERIAL;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Integer value) {
			return Integer.class;
		}

		@Nullable
		@Override
		public Integer extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return (Integer) set.getObject(name);
		}
	};

	TypeMapper<Integer, Integer> INTEGER = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(int.class) || type.equals(Integer.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.INTEGER;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Integer value) {
			return Integer.class;
		}

		@Nullable
		@Override
		public Integer extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return (Integer) set.getObject(name);
		}
	};

	TypeMapper<Long, Long> LONG = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(long.class) || type.equals(Long.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.BIG_INT;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Long value) {
			return Long.class;
		}

		@Nullable
		@Override
		public Long extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return (Long) set.getObject(name);
		}
	};

	TypeMapper<Double, Double> DOUBLE = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(double.class) || type.equals(Double.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.NUMERIC;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Double value) {
			return Double.class;
		}

		@Nullable
		@Override
		public Double extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return (Double) set.getObject(name);
		}
	};

	TypeMapper<byte[], byte[]> BLOB = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(byte[].class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.BYTE_ARRAY;
		}

		@NotNull
		@Override
		public Argument createArgument(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable byte[] value) {
			return new Argument() {
				@Override
				public void apply(int position, PreparedStatement statement, StatementContext ctx) throws SQLException {
					statement.setObject(position, value);
				}

				@Override
				public String toString() {
					return Base64.getEncoder().encodeToString(value);
				}
			};
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable byte[] value) {
			return byte[].class;
		}

		@Override
		public byte[] extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getBytes(name);
		}
	};

	TypeMapper<Boolean, Boolean> BOOLEAN = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(boolean.class) || type.equals(Boolean.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.BOOLEAN;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Boolean value) {
			return Boolean.class;
		}

		@Nullable
		@Override
		public Boolean extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return (Boolean) set.getObject(name);
		}
	};

	TypeMapper<String, String> STRING = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return ReflectionUtils.getClass(type).isAssignableFrom(String.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.TEXT;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable String value) {
			return String.class;
		}

		@Nullable
		@Override
		public String extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getString(name);
		}
	};

	TypeMapper<Instant, Instant> TIMESTAMP = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return ReflectionUtils.getClass(type).isAssignableFrom(Instant.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.TIMESTAMP;
		}

		@NotNull
		@Override
		public Argument createArgument(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Instant value) {
			return new Argument() {
				@Override
				public void apply(int position, PreparedStatement statement, StatementContext ctx) throws SQLException {
					statement.setTimestamp(position, value == null ? null : Timestamp.from(value));
				}

				@Override
				public String toString() {
					return Objects.toString(value);
				}
			};
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Instant value) {
			return Instant.class;
		}

		@Nullable
		@Override
		public Instant extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			var timestamp = set.getTimestamp(name);
			return timestamp == null ? null : timestamp.toInstant();
		}
	};

	TypeMapper<java.util.UUID, java.util.UUID> UUID = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(java.util.UUID.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.UUID;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable java.util.UUID value) {
			return java.util.UUID.class;
		}

		@NotNull
		@Override
		public UUID format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable java.util.UUID value) {
			return value == null ? java.util.UUID.randomUUID() : value;
		}

		@Nullable
		@Override
		public UUID extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			var temp = set.getString(name);
			return temp == null ? null : java.util.UUID.fromString(temp);
		}
	};

	TypeMapper<String, ID> MKID = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return type.equals(ID.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.TEXT;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable ID value) {
			return String.class;
		}

		@NotNull
		@Override
		public String format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable ID value) {
			return value == null ? ID.generate().asString() : value.asString();
		}

		@Nullable
		@Override
		public String extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getString(name);
		}

		@Nullable
		@Override
		public ID parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable String value) {
			return value == null ? null : ID.decode(value);
		}
	};

	TypeMapper<Object, Optional<?>> OPTIONAL = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field) {
			return type.equals(Optional.class);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return manager.getType(ReflectionUtils.getComponentType(type), f);
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Optional<?> value) {
			if(value == null) return Object.class;

			var p = ReflectionUtils.getComponentType(type);
			return manager.getMapper(p, f).getFormattedType(manager, type, f, value);
		}

		@Nullable
		@Override
		public Object format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Optional<?> value) {
			if(value == null) return null;

			var p = ReflectionUtils.getComponentType(type);
			return manager.getMapper(p, f).format(manager, p, f, value.orElse(null));
		}

		@Nullable
		@Override
		public Object extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getObject(name);
		}

		@NotNull
		@Override
		public Optional<?> parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable Object value) {
			var p = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
			return Optional.ofNullable(manager.parse(p, field, value));
		}
	};

	TypeMapper<String, Enum<?>> ENUM = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return ReflectionUtils.getClass(type).isEnum();
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.TEXT;
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Enum<?> value) {
			return String.class;
		}

		@Nullable
		@Override
		public String format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Enum<?> value) {
			return value == null ? null : value.name();
		}

		@Nullable
		@Override
		public String extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getString(name);
		}

		@Nullable
		@Override
		public Enum<?> parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable String value) {
			if(value == null) return null;
			return ReflectionUtils.getEnumConstant(type, value).orElse(null);
		}
	};

	TypeMapper<Object[], ?> ARRAY = new TypeMapper<>() {
		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return ReflectionUtils.isArray(type, true);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			var component = ReflectionUtils.getComponentType(type);
			return DataType.ofName(manager.getType(component, f).getName() + "[]");
		}

		@NotNull
		@Override
		public Argument createArgument(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Object[] value) {
			return new Argument() {
				@Override
				public void apply(int position, PreparedStatement statement, StatementContext ctx) throws SQLException {
					if(value == null) {
						statement.setArray(position, null);
						return;
					}

					var component = ReflectionUtils.getActualArrayComponent(type);
					var type = manager.getType(component, f);

					statement.setArray(position, statement.getConnection().createArrayOf(type.getName(), value));
				}

				@Override
				public String toString() {
					return Arrays.deepToString(value);
				}
			};
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Object value) {
			return ReflectionHelpers.getClazz(ReflectionUtils.getComponentType(type)).arrayType();
		}

		@Nullable
		@Override
		public Object[] format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Object value) {
			if(value == null) return null;

			var component = ReflectionUtils.getComponentType(type);

			if(ReflectionUtils.isArray(component, true)) {
				var maxLength = new AtomicInteger(0);
				var array = ReflectionUtils.stream(value)
						.map(x -> manager.format(component, f, x))
						.map(x -> x == null ? ReflectionUtils.createArray(component, 0) : (Object[]) x)
						.peek(x -> maxLength.set(Math.max(maxLength.get(), x.length)))
						.toList();

				return array.stream()
						.map(v -> Arrays.copyOf(v, maxLength.get()))
						.toArray(i -> ReflectionUtils.createArray(manager.getMapper(ReflectionUtils.getComponentType(component), f).getFormattedType(manager, type, f, null), i, maxLength.get()));
			} else {
				return ReflectionUtils.stream(value)
						.map(x -> manager.format(component, f, x))
						.toArray(i -> ReflectionUtils.createArray(manager.getMapper(component, f).getFormattedType(manager, type, f, null), i));
			}
		}

		@Nullable
		@Override
		public Object[] extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			var temp = set.getArray(name);
			if(temp == null) return null;

			else return (Object[]) temp.getArray();
		}

		@Nullable
		@Override
		public Object parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable Object[] value) {
			if(value == null) return null;

			var component = ReflectionUtils.getComponentType(type);

			var array = Arrays.stream(value)
					.filter(x -> ReflectionUtils.isArray(component, true) || x != null)
					.map(v -> manager.parse(component, field, v))
					.toList();

			return ReflectionUtils.isArray(type, false)
					? array.toArray(i -> ReflectionUtils.createArray(component, i))
					: ReflectionHelpers.createCollection(ReflectionUtils.getClass(type), ReflectionUtils.getClass(component), array);
		}
	};

	TypeMapper<String, ?> JSON = new TypeMapper<>() {
		public static final ToNumberStrategy numberStrategy = in -> {
			var str = in.nextString();
			return str.contains(".") ? Double.parseDouble(str) : Integer.parseInt(str);
		};

		private final static Gson gson = new GsonBuilder()
				.setNumberToNumberStrategy(numberStrategy)
				.setObjectToNumberStrategy(numberStrategy)
				.create();

		@Override
		public boolean accepts(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return f.isAnnotationPresent(Json.class);
		}

		@NotNull
		@Override
		public Argument createArgument(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable String value) {
			return new Argument() {
				@Override
				public void apply(int position, PreparedStatement statement, StatementContext ctx) throws SQLException {
					statement.setString(position, value);
				}

				@Override
				public String toString() {
					return Objects.toString(value);
				}
			};
		}

		@NotNull
		@Override
		public Type getFormattedType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Object value) {
			return String.class;
		}

		@NotNull
		@Override
		public String format(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f, @Nullable Object value) {
			return gson.toJson(value);
		}

		@NotNull
		@Override
		public DataType getType(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field f) {
			return PostgresType.TEXT;
		}

		@Nullable
		@Override
		public String extract(@NotNull ResultSet set, @NotNull String name, @NotNull Type target) throws SQLException {
			return set.getString(name);
		}

		@Nullable
		@Override
		public Object parse(@NotNull DatabaseManager manager, @NotNull Type type, @NotNull Field field, @Nullable String value) {
			return value == null ? null : gson.fromJson(value, type);
		}
	};
}
