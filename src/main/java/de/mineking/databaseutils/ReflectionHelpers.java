package de.mineking.databaseutils;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.*;

public class ReflectionHelpers {
	private ReflectionHelpers() {}

	@SuppressWarnings("unchecked")
	public static <D> Class<D> getClazz(@NotNull Type type) {
		if (type instanceof Class<?> c) return (Class<D>) c;
		if (type instanceof ParameterizedType pt) return getClazz(pt.getRawType());
		if (type instanceof GenericArrayType gt) return (Class<D>) getClazz(gt.getGenericComponentType()).arrayType();
		if (type instanceof WildcardType) return (Class<D>) void.class;

		throw new IllegalArgumentException("Cannot find Class for " + type);
	}

	@SuppressWarnings("unchecked")
	public static <C> Collection<C> createCollection(Class<?> type, Class<?> component, List<C> array) {
		if(type.isAssignableFrom(List.class)) return new ArrayList<>(array);
		else if(type.isAssignableFrom(Set.class)) return new HashSet<>(array);
		else if(type.isAssignableFrom(EnumSet.class)) return (Collection<C>) createEnumSet(array, component);

		throw new IllegalStateException("Cannot create collection for " + type.getTypeName() + " with component " + component.getTypeName());
	}

	@SuppressWarnings("unchecked")
	public static <E extends Enum<E>> EnumSet<E> createEnumSet(Collection<?> collection, Class<?> component) {
		return collection.isEmpty() ? EnumSet.noneOf((Class<E>) component) : EnumSet.copyOf((Collection<E>) collection);
	}
}
