package de.mineking.databaseutils.type;

import org.jetbrains.annotations.NotNull;

public interface DataType {
	@NotNull
	static DataType ofName(@NotNull String name) {
		return new DataType() {
			@NotNull
			@Override
			public String getName() {
				return name;
			}

			@Override
			public String toString() {
				return name;
			}
		};
	}

	@NotNull
	String getName();
}
