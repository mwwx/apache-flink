/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dialect;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link JdbcDialects}.
 */
public class JdbcDialectsTest {

	private static final String MYSQL_DB_URL = "jdbc:mysql:xxxxx";
	private static final String DERBY_DB_URL = "jdbc:derby:xxxxx";
	private static final String POSTGRES_DB_URL = "jdbc:postgresql:xxxxx";
	private static final String COMMONS_DB_URL = "jdbc:linkoopdb:xxxxx";

	private static final String TABLE_NAME = "SCHEMA.TEST";
	/*private static final String USERNAME = "USERNAME";
	private static final String PASSWORD = "PASSWORD";
	private static final String DRIVER = "DRIVER";*/
	private static final String[] FIELD_NAMES = new String[] {"name", "age"};
	private static final String[] CONIDITION_NAMES = new String[] {"name"};

	@Test
	public void testMysqlQuery() {
		Assert.assertTrue(JdbcDialects.get(MYSQL_DB_URL).isPresent());
		JdbcDialect dialect = JdbcDialects.get(MYSQL_DB_URL).get();
		Assert.assertEquals("INSERT INTO SCHEMA.TEST(name, age) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST WHERE name=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE SCHEMA.TEST SET name=?, age=? WHERE name=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM SCHEMA.TEST WHERE name=? AND age=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));

		dialect.setQuoting(true);
		Assert.assertEquals("INSERT INTO `SCHEMA`.`TEST`(`name`, `age`) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT `name`, `age` FROM `SCHEMA`.`TEST`",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT `name`, `age` FROM `SCHEMA`.`TEST`",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT `name`, `age` FROM `SCHEMA`.`TEST` WHERE `name`=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE `SCHEMA`.`TEST` SET `name`=?, `age`=? WHERE `name`=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM `SCHEMA`.`TEST` WHERE `name`=? AND `age`=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));
	}

	@Test
	public void testDerbyQuery() {
		Assert.assertTrue(JdbcDialects.get(DERBY_DB_URL).isPresent());
		JdbcDialect dialect = JdbcDialects.get(DERBY_DB_URL).get();
		Assert.assertEquals("INSERT INTO SCHEMA.TEST(name, age) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST WHERE name=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE SCHEMA.TEST SET name=?, age=? WHERE name=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM SCHEMA.TEST WHERE name=? AND age=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));

		dialect.setQuoting(true);
		Assert.assertEquals("INSERT INTO SCHEMA.TEST(name, age) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST WHERE name=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE SCHEMA.TEST SET name=?, age=? WHERE name=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM SCHEMA.TEST WHERE name=? AND age=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));
	}

	@Test
	public void testPostgresQuery() {
		Assert.assertTrue(JdbcDialects.get(POSTGRES_DB_URL).isPresent());
		JdbcDialect dialect = JdbcDialects.get(POSTGRES_DB_URL).get();
		Assert.assertEquals("INSERT INTO SCHEMA.TEST(name, age) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST WHERE name=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE SCHEMA.TEST SET name=?, age=? WHERE name=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM SCHEMA.TEST WHERE name=? AND age=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));

		dialect.setQuoting(true);
		Assert.assertEquals("INSERT INTO \"SCHEMA\".\"TEST\"(\"name\", \"age\") VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\"",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\"",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\" WHERE \"name\"=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE \"SCHEMA\".\"TEST\" SET \"name\"=?, \"age\"=? WHERE \"name\"=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM \"SCHEMA\".\"TEST\" WHERE \"name\"=? AND \"age\"=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));
	}

	@Test
	public void testCommonsQuery() {
		JdbcDialect dialect = CommonDialect.newInstance(COMMONS_DB_URL);
		Assert.assertEquals("INSERT INTO SCHEMA.TEST(name, age) VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT name, age FROM SCHEMA.TEST WHERE name=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE SCHEMA.TEST SET name=?, age=? WHERE name=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM SCHEMA.TEST WHERE name=? AND age=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));

		dialect.setQuoting(true);
		Assert.assertEquals("INSERT INTO \"SCHEMA\".\"TEST\"(\"name\", \"age\") VALUES (?, ?)",
			dialect.getInsertIntoStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\"",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, new String[0]));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\"",
			dialect.getSelectAllFromStatement(TABLE_NAME, FIELD_NAMES));
		Assert.assertEquals("SELECT \"name\", \"age\" FROM \"SCHEMA\".\"TEST\" WHERE \"name\"=?",
			dialect.getSelectFromStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("UPDATE \"SCHEMA\".\"TEST\" SET \"name\"=?, \"age\"=? WHERE \"name\"=?",
			dialect.getUpdateStatement(TABLE_NAME, FIELD_NAMES, CONIDITION_NAMES));
		Assert.assertEquals("DELETE FROM \"SCHEMA\".\"TEST\" WHERE \"name\"=? AND \"age\"=?",
			dialect.getDeleteStatement(TABLE_NAME, FIELD_NAMES));
	}
}
