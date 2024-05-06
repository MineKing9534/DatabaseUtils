package database;

import de.mineking.javautils.ID;
import de.mineking.databaseutils.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WhereTest {
	private final DatabaseManager manager;
	private final Table<TestClass> table;

	@ToString
	@NoArgsConstructor
	@AllArgsConstructor
	private class TestClass implements DataClass<TestClass> {
		@Column(key = true)
		public ID id;

		@Column
		public String test;

		@Column
		public List<String> array;

		@NotNull
		@Override
		public Table<TestClass> getTable() {
			return table;
		}
	}

	public WhereTest() {
		manager = new DatabaseManager("jdbc:postgresql://localhost:5433/postgres", "postgres", "postgres");
		table = manager.getTable(TestClass.class, TestClass::new, "whereTest").createTable();

		manager.getDriver().setSqlLogger(new SqlLogger() {
			@Override
			public void logBeforeExecution(StatementContext context) {
				System.out.println(context.getParsedSql().getSql());
				System.out.println(context.getBinding());
			}
		});

		table.deleteAll();

		table.insert(new TestClass(null, "a", List.of("a")));
		table.insert(new TestClass(null, "a", List.of("a", "b")));
		table.insert(new TestClass(null, "b", List.of("a", "c")));
		table.insert(new TestClass(null, "b", List.of("b", "c")));
		table.insert(new TestClass(null, "c", List.of("b", "d")));
		table.insert(new TestClass(null, "d", List.of("c")));
		table.insert(new TestClass(null, "e", List.of("c", "d")));
	}

	@Test
	public void in() {
		assertEquals(0, table.selectMany(Where.valueContainsField("test", List.of())).size());
		assertEquals(2, table.selectMany(Where.valueContainsField("test", List.of("a"))).size());
		assertEquals(2, table.selectMany(Where.valueContainsField("test", List.of("b"))).size());
		assertEquals(4, table.selectMany(Where.valueContainsField("test", List.of("a", "b"))).size());
		assertEquals(5, table.selectMany(Where.valueContainsField("test", List.of("a", "b", "c"))).size());
	}

	@Test
	public void between() {
		assertEquals(2, table.selectMany(Where.between("test", "a", "a")).size());
		assertEquals(5, table.selectMany(Where.between("test", "a", "c")).size());
		assertEquals(3, table.selectMany(Where.between("test", "c", "e")).size());
	}

	@Test
	public void contains() {
		assertEquals(3, table.selectMany(Where.fieldContainsValue("array", "a")).size());
		assertEquals(3, table.selectMany(Where.fieldContainsValue("array", "b")).size());
		assertEquals(4, table.selectMany(Where.fieldContainsValue("array", "c")).size());
		assertEquals(2, table.selectMany(Where.fieldContainsValue("array", "d")).size());
	}
}
