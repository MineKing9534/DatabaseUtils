package de.mineking.databaseutils;

import de.mineking.databaseutils.type.DataType;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.argument.Argument;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class DatabaseManager {
	public static Function<Class<?>, ClassLoader> DEFAULT_LOADER = Class::getClassLoader;

	private final Map<String, Object> data = new HashMap<>();

	final List<TypeMapper<?, ?>> mappers = new ArrayList<>();
	final Jdbi db;

	public DatabaseManager(@NotNull String host, @NotNull String user, @NotNull String password) {
		db = Jdbi.create(host, user, password);

		mappers.add(TypeMapper.JSON);
		mappers.add(TypeMapper.SERIAL);
		mappers.add(TypeMapper.INTEGER);
		mappers.add(TypeMapper.LONG);
		mappers.add(TypeMapper.DOUBLE);
		mappers.add(TypeMapper.BLOB);
		mappers.add(TypeMapper.BOOLEAN);
		mappers.add(TypeMapper.STRING);
		mappers.add(TypeMapper.TIMESTAMP);
		mappers.add(TypeMapper.UUID);
		mappers.add(TypeMapper.MKID);
		mappers.add(TypeMapper.OPTIONAL);
		mappers.add(TypeMapper.ENUM);
		mappers.add(TypeMapper.ARRAY);
	}

	@NotNull
	public Jdbi getDriver() {
		return db;
	}

	@NotNull
	public DatabaseManager addMapper(@NotNull TypeMapper<?, ?> mapper) {
		mappers.add(0, mapper);
		return this;
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T, R> TypeMapper<T, R> getMapper(@NotNull Type type, @NotNull Field f) {
		return (TypeMapper<T, R>) mappers.stream()
				.filter(m -> m.accepts(this, type, f))
				.findFirst().orElseThrow(() -> new IllegalStateException("No mapper found for " + type));
	}

	@NotNull
	public DataType getType(@NotNull Type type, @NotNull Field field) {
		return getMapper(type, field).getType(this, type, field);
	}

	@Nullable
	public <T> Argument getArgument(@NotNull Type type, @NotNull Field field, @Nullable T value) {
		var mapper = getMapper(type, field);
		return mapper.createArgument(this, type, field, mapper.format(this, type, field, value));
	}

	@Nullable
	public Object format(@NotNull Type type, @NotNull Field field, @Nullable Object obj) {
		return getMapper(type, field).format(this, type, field, obj);
	}

	@Nullable
	public Object extract(@NotNull Type type, @NotNull Field field, @NotNull String name, @NotNull ResultSet set) throws SQLException {
		return getMapper(type, field).extract(set, name, type);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T, R> R parse(@NotNull Type type, @NotNull Field field, T value) {
		return (R) getMapper(type, field).parse(this, type, field, value);
	}

	public class TableBuilder<O, T extends Table<O>> {
		private final Class<O> type;
		private final Supplier<O> instance;

		private String name;
		private Class<? extends Table<O>> table;
		private ClassLoader loader;

		public TableBuilder(Class<O> type, Supplier<O> instance) {
			this.type = type;
			this.instance = instance;
		}

		@NotNull
		public TableBuilder<O, T> name(@NotNull String name) {
			this.name = name;
			return this;
		}

		@NotNull
		public TableBuilder<O, T> loader(@NotNull ClassLoader loader) {
			this.loader = loader;
			return this;
		}

		@NotNull
		@SuppressWarnings("unchecked")
		public <N extends Table<O>> TableBuilder<O, N> table(@NotNull Class<N> table) {
			this.table = table;
			return (TableBuilder<O, N>) this;
		}

		@NotNull
		@SuppressWarnings("unchecked")
		public T get() {
			return (T) Proxy.newProxyInstance(
					loader == null ? DEFAULT_LOADER.apply(type) : loader,
					new Class<?>[] { table == null ? Table.class : table },
					new TableImpl<>(DatabaseManager.this, type, instance, name == null ? type.getSimpleName().toLowerCase() : name)
			);
		}

		@NotNull
		public T create() {
			T table = get();
			table.createIfNotExists();
			return table;
		}
	}

	@NotNull
	public <O> TableBuilder<O, ?> getTable(@NotNull Class<O> type, @NotNull Supplier<O> instance) {
		return new TableBuilder<>(type, instance);
	}

	@NotNull
	public DatabaseManager putData(@NotNull String name, @NotNull Object value) {
		data.put(name, value);
		return this;
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T getData(@NotNull String name) {
		return (T) data.get(name);
	}
}
