package org.talend.components.common.tableaction;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultSQLCreateTableActionTest {

    private static Schema schema = SchemaBuilder.builder()
            .record("main")
            .fields()
            .name("id")
            .prop(SchemaConstants.TALEND_COLUMN_IS_KEY, "true")
            .type(AvroUtils._int())
            .withDefault(1)
            .name("name")
            .prop(SchemaConstants.TALEND_COLUMN_IS_KEY, "true")
            .prop(SchemaConstants.TALEND_COLUMN_DB_LENGTH, "255")
            .prop(SchemaConstants.TALEND_COLUMN_DEFAULT, "\"ok\"")
            .type(AvroUtils._string())
            .noDefault()
            .name("date")
            .type(AvroUtils._logicalDate())
            .noDefault()
            .name("salary")
            .prop(SchemaConstants.TALEND_COLUMN_DB_TYPE, "MY_DOUBLE")
            .prop(SchemaConstants.TALEND_COLUMN_PRECISION, "38")
            .prop(SchemaConstants.TALEND_COLUMN_SCALE, "4")
            .type(AvroUtils._double())
            .withDefault("0")
            .name("updated")
            .type(AvroUtils._logicalTimestamp())
            .noDefault()
            .endRecord();

    @Test
    public void createTable() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction("MyTable", schema, false, false, false);
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals(
                    "CREATE TABLE MyTable (id NUMERIC, name VARCHAR(255) DEFAULT \"ok\", date DATE, salary MY_DOUBLE(38, 4), updated TIMESTAMP, CONSTRAINT pk_MyTable PRIMARY KEY (id, name))",
                    queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTableIfNotExists() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction("MyTable", schema, true, false, false);
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals(
                    "CREATE TABLE IF NOT EXISTS MyTable (id NUMERIC, name VARCHAR(255) DEFAULT \"ok\", date DATE, salary MY_DOUBLE(38, 4), updated TIMESTAMP, CONSTRAINT pk_MyTable PRIMARY KEY (id, name))",
                    queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropNCreateTable() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction("MyTable", schema, false, true, false);
        try {
            List<String> queries = action.getQueries();
            assertEquals(2, queries.size());
            assertEquals("DROP TABLE MyTable CASCADE", queries.get(0));
            assertEquals(
                    "CREATE TABLE MyTable (id NUMERIC, name VARCHAR(255) DEFAULT \"ok\", date DATE, salary MY_DOUBLE(38, 4), updated TIMESTAMP, CONSTRAINT pk_MyTable PRIMARY KEY (id, name))",
                    queries.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropIfExistsNCreateTable() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction("MyTable", schema, false, false, true);
        try {
            List<String> queries = action.getQueries();
            assertEquals(2, queries.size());
            assertEquals("DROP TABLE IF EXISTS MyTable CASCADE", queries.get(0));
            assertEquals(
                    "CREATE TABLE MyTable (id NUMERIC, name VARCHAR(255) DEFAULT \"ok\", date DATE, salary MY_DOUBLE(38, 4), updated TIMESTAMP, CONSTRAINT pk_MyTable PRIMARY KEY (id, name))",
                    queries.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clearNCreateTable() {
        DefaultSQLClearTableAction action =
                new DefaultSQLClearTableAction("MyTable");
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals("DELETE FROM MyTable", queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void truncateNCreateTable() {
        DefaultSQLTruncateTableAction action = new DefaultSQLTruncateTableAction("MyTable");
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals("TRUNCATE TABLE MyTable", queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}