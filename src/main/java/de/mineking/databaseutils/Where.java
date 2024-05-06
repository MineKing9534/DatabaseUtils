package de.mineking.databaseutils;

import de.mineking.javautils.ID;
import de.mineking.javautils.Pair;
import de.mineking.javautils.reflection.ReflectionUtils;
import org.jdbi.v3.core.argument.Argument;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface Where {
	@NotNull
	static <T> Where identify(@NotNull Table<T> table, @NotNull T object) {
		if(table.getKeys().isEmpty()) throw new IllegalArgumentException("Cannot identify object without keys");
		return allOf(table.getKeys().entrySet().stream()
				.map(e -> {
					try {
						return equals(e.getKey(), e.getValue().get(object));
					} catch(IllegalAccessException ex) {
						throw new RuntimeException(ex);
					}
				})
				.toList()
		);
	}

	@NotNull
	static <T> Where detectConflict(@NotNull Table<T> table, @NotNull T object, boolean isInsert) {
		if(table.getUnique().isEmpty()) return empty();
		var temp = anyOf(table.getUnique().entrySet().stream()
				.filter(e -> !e.getValue().getAnnotation(Column.class).key())
				.map(e -> {
					try {
						return equals(e.getKey(), e.getValue().get(object));
					} catch(IllegalAccessException ex) {
						throw new RuntimeException(ex);
					}
				})
				.toList()
		);

		return isInsert
				? temp.or(identify(table, object))
				: temp.and(not(identify(table, object)));
	}

	@NotNull
	static Where empty() {
		return new Where() {
			@NotNull
			@Override
			public Map<String, ArgumentFactory> values() {
				return Collections.emptyMap();
			}

			@NotNull
			@Override
			public String get() {
				return "";
			}

			@NotNull
			@Override
			public String format() {
				return "";
			}

			@NotNull
			@Override
			public Where and(@NotNull Where other) {
				return other;
			}

			@NotNull
			@Override
			public Where or(@NotNull Where other) {
				return other;
			}

			@NotNull
			@Override
			public Where not() {
				return this;
			}
		};
	}

	@NotNull
	static Where allOf(@NotNull Where where, @NotNull Where... others) {
		for(var w : others) where = where.and(w);
		return where;
	}

	@NotNull
	static Where allOf(@NotNull Collection<Where> wheres) {
		return allOf(empty(), wheres.toArray(Where[]::new));
	}

	@NotNull
	static Where anyOf(@NotNull Where where, @NotNull Where... others) {
		for(var w : others) where = where.or(w);
		return where;
	}

	@NotNull
	static Where anyOf(@NotNull Collection<Where> wheres) {
		return anyOf(empty(), wheres.toArray(Where[]::new));
	}

	@NotNull
	static Where noneOf(@NotNull Where where, @NotNull Where... others) {
		return anyOf(where, others).not();
	}

	@NotNull
	static Where noneOf(@NotNull Collection<Where> wheres) {
		return noneOf(empty(), wheres.toArray(Where[]::new));
	}

	@NotNull
	static Where equals(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "=");
	}

	@NotNull
	static Where notEqual(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "!=");
	}

	@NotNull
	static Where like(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "like");
	}

	@NotNull
	static Where likeIgnoreCase(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "ilike");
	}

	@NotNull
	static Where greater(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, ">");
	}

	@NotNull
	static Where lower(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "<");
	}

	@NotNull
	static Where greaterOrEqual(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, ">=");
	}

	@NotNull
	static Where lowerOrEqual(@NotNull String name, @Nullable Object value) {
		return WhereImpl.create(name, value, "<=");
	}

	@NotNull
	static Where valueContainsField(@NotNull String name, @NotNull Collection<?> value) {
		if(value.isEmpty()) return FALSE();

		var id = ID.generate().asString();
		return new WhereImpl(name + " = any(:" + id + ")", Map.of(id, ArgumentFactory.create(name, value, table -> {
			var f = table.getColumns().get(name);
			if(f == null) throw new IllegalStateException("Table has no column with name '" + name + "'");

			var type = ReflectionHelpers.getClazz(f.getGenericType()).arrayType();
			var mapper = table.getManager().getMapper(type, f);

			return mapper.createArgument(table.getManager(), type, f, mapper.format(table.getManager(), type, f, value));
		})));
	}

	@NotNull
	static Where fieldContainsValue(@NotNull String name, @Nullable Object value) {
		var id = ID.generate().asString();
		return new WhereImpl(":" + id + " = any(\"" + name + "\")", Map.of(id, ArgumentFactory.create(name, value, table -> {
			var f = table.getColumns().get(name);
			if(f == null) throw new IllegalStateException("Table has no column with name '" + name + "'");

			var type = ReflectionUtils.getActualArrayComponent(f.getGenericType());
			var mapper = table.getManager().getMapper(type, f);

			Object v;

			try {
				v = mapper.format(table.getManager(), type, f, value);
			} catch(IllegalArgumentException | ClassCastException ex) {
				v = value;
			}

			return mapper.createArgument(table.getManager(), type, f, v);
		})));
	}

	@NotNull
	static Where between(@NotNull String name, @NotNull Object lower, @NotNull Object upper) {
		return WhereImpl.create(name, List.of(lower, upper), "between", Collectors.joining(" and "));
	}

	@NotNull
	static Where isNull(@NotNull String name) {
		return Where.unsafe(name + " is null");
	}

	@NotNull
	static Where isNotNull(@NotNull String name) {
		return Where.unsafe(name + " is not null");
	}

	@NotNull
	static Where not(@NotNull Where where) {
		return where.not();
	}

	@NotNull
	static Where TRUE() {
		return Where.unsafe("TRUE");
	}

	@NotNull
	static Where FALSE() {
		return Where.unsafe("FALSE");
	}

	@NotNull
	default Where and(@NotNull Where other) {
		return WhereImpl.combined(this, other, "and");
	}

	@NotNull
	default Where or(@NotNull Where other) {
		return WhereImpl.combined(this, other, "or");
	}

	@NotNull
	default Where not() {
		return new WhereImpl("not " + get(), values());
	}

	@NotNull
	Map<String, ArgumentFactory> values();

	default Map<String, Argument> formatValues(@NotNull Table<?> table) {
		return values().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().create(table)));
	}

	@NotNull
	String get();

	@NotNull
	default String format() {
		return "where " + get();
	}

	@NotNull
	static Where unsafe(@NotNull String str) {
		return new Where() {
			@NotNull
			@Override
			public Map<String, ArgumentFactory> values() {
				return Collections.emptyMap();
			}

			@NotNull
			@Override
			public String get() {
				return str;
			}

			@Override
			public String toString() {
				return str;
			}
		};
	}

	class WhereImpl implements Where {
		private final String str;
		private final Map<String, ArgumentFactory> values;

		public WhereImpl(String str, Map<String, ArgumentFactory> values) {
			this.str = str;
			this.values = values;
		}

		public static Where create(String name, Object value, String operator) {
			var id = ID.generate().asString();
			return new WhereImpl("\"" + name + "\" " + operator + " :" + id, Map.of(id, ArgumentFactory.createDefault(name, value)));
		}

		public static Where create(String name, List<Object> values, String operator, Collector<CharSequence, ?, String> collector) {
			var ids = values.stream()
					.map(v -> new Pair<>(ID.generate().asString(), v))
					.toList();
			return new WhereImpl("\"" + name + "\" " + operator + " " + ids.stream().map(p -> ":" + p.key()).collect(collector),
					ids.stream().collect(Collectors.toMap(Pair::key, p -> ArgumentFactory.createDefault(name, p.value())))
			);
		}

		public static Where combined(Where a, Where b, String operator) {
			if(a.get().isEmpty()) return b;
			if(b.get().isEmpty()) return a;

			var combined = new HashMap<>(a.values());
			combined.putAll(b.values());

			return new WhereImpl("(" + a.get() + ") " + operator + " (" + b.get() + ")", combined);
		}

		@NotNull
		@Override
		public Map<String, ArgumentFactory> values() {
			return values;
		}

		@NotNull
		@Override
		public String get() {
			return str;
		}

		@Override
		public String toString() {
			return get();
		}
	}
}
