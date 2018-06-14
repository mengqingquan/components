package org.talend.components.common.tableaction;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;

import java.util.List;

import static org.junit.Assert.*;

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
    public void getQueriesTest(){
        DefaultSQLCreateTableAction action = new DefaultSQLCreateTableAction("MyTable", schema);
        try {
            List<String> queries = action.getQueries();
            assertEquals(1, queries.size());
            assertEquals("CREATE TABLE MyTable (id NUMERIC, name VARCHAR(255) DEFAULT \"ok\", date DATE, salary MY_DOUBLE(38, 4), updated TIMESTAMP, CONSTRAINT pk_MyTable PRIMARY KEY (id, name))", queries.get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}