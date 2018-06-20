package org.talend.components.common.tableaction;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

import java.sql.Types;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultSQLCreateTableActionTest {

    private static Schema schema;

    @Before
    public void createSchema(){
        schema = SchemaBuilder.builder()
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
    }

    @Test
    public void createTable() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, false, false, false);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        action.setConfig(conf);
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
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, true, false, false);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        action.setConfig(conf);
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
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, false, true, false);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        conf.SQL_DROP_TABLE_SUFFIX = " CASCADE";
        action.setConfig(conf);

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
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, false, false, true);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        conf.SQL_DROP_TABLE_SUFFIX = " CASCADE";
        action.setConfig(conf);

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
    public void dropIfExistsNCreateTableUppercase() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, false, false, true);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        conf.SQL_DROP_TABLE_SUFFIX = " CASCADE";
        conf.SQL_UPPERCASE_IDENTIFIER = true;
        action.setConfig(conf);

        try {
            List<String> queries = action.getQueries();
            assertEquals(2, queries.size());
            assertEquals("DROP TABLE IF EXISTS MYTABLE CASCADE", queries.get(0));
            assertEquals(
                    "CREATE TABLE MYTABLE (ID NUMERIC, NAME VARCHAR(255) DEFAULT \"ok\", DATE DATE, SALARY MY_DOUBLE(38, 4), UPDATED TIMESTAMP, CONSTRAINT pk_MYTABLE PRIMARY KEY (ID, NAME))",
                    queries.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropIfExistsNCreateTableWithConfig() {
        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, true, false, true);
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        conf.SQL_DROP_TABLE_PREFIX = "SQL_DROP_TABLE_PREFIX ";
        conf.SQL_DROP_TABLE = "SQL_DROP_TABLE";
        conf.SQL_DROP_TABLE_IF_EXISITS = "SQL_DROP_TABLE_IF_EXISITS";
        conf.SQL_DROP_TABLE_SUFFIX = " SQL_DROP_TABLE_SUFFIX";

        conf.SQL_CREATE_TABLE_PREFIX = "SQL_CREATE_TABLE_PREFIX ";
        conf.SQL_CREATE_TABLE = "SQL_CREATE_TABLE";
        conf.SQL_CREATE_TABLE_IF_NOT_EXISTS = "SQL_CREATE_TABLE_IF_NOT_EXISTS";
        conf.SQL_CREATE_TABLE_DEFAULT = "SQL_CREATE_TABLE_DEFAULT";
        conf.SQL_CREATE_TABLE_CONSTRAINT = "SQL_CREATE_TABLE_CONSTRAINT";
        conf.SQL_CREATE_TABLE_PRIMARY_KEY_PREFIX = "SQL_CREATE_TABLE_PRIMARY_KEY_PREFIX";
        conf.SQL_CREATE_TABLE_PRIMARY_KEY = "SQL_CREATE_TABLE_PRIMARY_KEY";
        conf.SQL_CREATE_TABLE_PRIMARY_KEY_ENCLOSURE_START = "[";
        conf.SQL_CREATE_TABLE_PRIMARY_KEY_ENCLOSURE_END = "]";
        conf.SQL_CREATE_TABLE_FIELD_SEP = "| ";
        conf.SQL_CREATE_TABLE_FIELD_ENCLOSURE_START = "{";
        conf.SQL_CREATE_TABLE_FIELD_ENCLOSURE_END = "}";
        conf.SQL_CREATE_TABLE_LENGTH_START = "<";
        conf.SQL_CREATE_TABLE_LENGTH_END = ">";
        conf.SQL_CREATE_TABLE_PRECISION_START = "/";
        conf.SQL_CREATE_TABLE_PRECISION_END = "\\";
        conf.SQL_CREATE_TABLE_SCALE_SEP = "#";


        action.setConfig(conf);

        try {
            List<String> queries = action.getQueries();
            assertEquals(2, queries.size());
            assertEquals("SQL_DROP_TABLE_PREFIX SQL_DROP_TABLE SQL_DROP_TABLE_IF_EXISITS MyTable SQL_DROP_TABLE_SUFFIX", queries.get(0));
            assertEquals(
                    "SQL_CREATE_TABLE_PREFIX SQL_CREATE_TABLE SQL_CREATE_TABLE_IF_NOT_EXISTS MyTable {id NUMERIC| name VARCHAR<255> SQL_CREATE_TABLE_DEFAULT \"ok\"| date DATE| salary MY_DOUBLE/38#4\\| updated TIMESTAMP| SQL_CREATE_TABLE_CONSTRAINT SQL_CREATE_TABLE_PRIMARY_KEY_PREFIXMyTable SQL_CREATE_TABLE_PRIMARY_KEY [id| name]}",
                    queries.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clearNCreateTable() {
        DefaultSQLClearTableAction action =
                new DefaultSQLClearTableAction(new String[]{"MyTable"});
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        action.setConfig(conf);
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
        DefaultSQLTruncateTableAction action = new DefaultSQLTruncateTableAction(new String[]{"MyTable"});
        TableActionConfig conf = new TableActionConfig();
        conf.SQL_ESCAPE_ENABLED = false;
        action.setConfig(conf);
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals("TRUNCATE TABLE MyTable", queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void truncateNCreateTableWithFullName() {
        DefaultSQLTruncateTableAction action = new DefaultSQLTruncateTableAction(new String[]{"mydatabase","myschema","MyTable"});
        TableActionConfig conf = new TableActionConfig();
        action.setConfig(conf);
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals("TRUNCATE TABLE \"mydatabase\".\"myschema\".\"MyTable\"", queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropIfExistsNCreateTableUppercaseWithSQLConf() {
        schema = SchemaBuilder.builder()
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
                .name("myvariant")
                .type(SchemaBuilder.builder().record("record").fields().endRecord())
                .noDefault()
                .endRecord();


        DefaultSQLCreateTableAction action =
                new DefaultSQLCreateTableAction(new String[]{"MyTable"}, schema, false, false, true);
        TableActionConfig conf = new TableActionConfig();

        conf.CONVERT_JAVATYPE_TO_SQLTYPE.put("java.util.Date", -50001);
        conf.CONVERT_LOGICALTYPE_TO_SQLTYPE.put(LogicalTypes.date(), -50001);
        conf.CONVERT_AVROTYPE_TO_SQLTYPE.put(Schema.Type.RECORD, -50002);
        conf.CUSTOMIZE_SQLTYPE_TYPENAME.put(-50001, "datetime_tz");
        conf.CUSTOMIZE_SQLTYPE_TYPENAME.put(-50002, "VARIANT");

        conf.SQL_DROP_TABLE_SUFFIX = " CASCADE";
        conf.SQL_UPPERCASE_IDENTIFIER = true;
        conf.SQL_ESCAPE_ENABLED = false;

        action.setConfig(conf);

        try {
            List<String> queries = action.getQueries();
            assertEquals(2, queries.size());
            assertEquals("DROP TABLE IF EXISTS MYTABLE CASCADE", queries.get(0));
            assertEquals(
                    "CREATE TABLE MYTABLE (ID NUMERIC, NAME VARCHAR(255) DEFAULT \"ok\", DATE DATETIME_TZ, SALARY MY_DOUBLE(38, 4), UPDATED TIMESTAMP, MYVARIANT VARIANT, CONSTRAINT pk_MYTABLE PRIMARY KEY (ID, NAME))",
                    queries.get(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}