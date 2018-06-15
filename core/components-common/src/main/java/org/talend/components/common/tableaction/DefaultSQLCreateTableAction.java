// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.common.tableaction;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.daikon.avro.SchemaConstants;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public class DefaultSQLCreateTableAction implements TableAction {

    private final Logger log = LoggerFactory.getLogger(DefaultSQLCreateTableAction.class);

    private String table;

    private Schema schema;

    private boolean drop;

    private boolean createIfNotExists;
    private boolean dropIfExists;

    public DefaultSQLCreateTableAction(final String table, final Schema schema, boolean createIfNotExists, boolean drop,
            boolean dropIfExists) {
        if (table == null || table.isEmpty()) {
            throw new InvalidParameterException("Table name can't null or empty");
        }

        this.table = table;
        this.schema = schema;
        this.createIfNotExists = createIfNotExists;

        this.drop = drop;
        this.dropIfExists = dropIfExists;
        if(dropIfExists){
            this.drop = true;
        }

    }

    @Override
    public List<String> getQueries() throws Exception {
        List<String> queries = new ArrayList<>();

        if (drop) {
            queries.add(getDropTableQuery());
        }

        queries.add(getCreateTableQuery());

        if (log.isDebugEnabled()) {
            log.debug("Generated SQL queries for create table:");
            for (String q : queries) {
                log.debug(q);
            }
        }

        return queries;
    }

    private String getDropTableQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE ");
        if(dropIfExists){
            sb.append("IF EXISTS ");
        }
        sb.append(table);
        sb.append(" CASCADE");

        return sb.toString();
    }

    private String getCreateTableQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");

        if(createIfNotExists){
            sb.append("IF NOT EXISTS ");
        }

        sb.append(table);
        sb.append(" (");
        sb.append(buildColumns());
        sb.append(")");

        return sb.toString();
    }

    private StringBuilder buildColumns() {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        List<Schema.Field> fields = schema.getFields();
        List<String> keys = new ArrayList<>();
        for (Schema.Field f : fields) {
            if (!first) {
                sb.append(", ");
            }

            String sDBLength = f.getProp(SchemaConstants.TALEND_COLUMN_DB_LENGTH);
            String sDBName = f.getProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME);
            String sDBType = f.getProp(SchemaConstants.TALEND_COLUMN_DB_TYPE);
            String sDBDefault = f.getProp(SchemaConstants.TALEND_COLUMN_DEFAULT);
            String sDBPrecision = f.getProp(SchemaConstants.TALEND_COLUMN_PRECISION);
            String sDBScale = f.getProp(SchemaConstants.TALEND_COLUMN_SCALE);
            boolean sDBIsKey = Boolean.valueOf(f.getProp(SchemaConstants.TALEND_COLUMN_IS_KEY)).booleanValue();

            String name = sDBName == null ? f.name() : sDBName;
            if (sDBIsKey) {
                keys.add(name);
            }
            sb.append(name);
            sb.append(" ");

            if (isNullOrEmpty(sDBType)) {
                // If DB type not set, try to guess it
                sDBType = ConvertAvroTypeToSQL.convertToSQLTypeString(f.schema());
            }
            sb.append(sDBType);

            // Length
            if (!isNullOrEmpty(sDBLength)) {
                sb.append("(");
                sb.append(sDBLength);
                sb.append(")");
            } else if (!isNullOrEmpty(sDBPrecision)) { // or precision/scale
                sb.append("(");
                sb.append(sDBPrecision);
                if (!isNullOrEmpty(sDBScale)) {
                    sb.append(", ");
                    sb.append(sDBScale);
                }
                sb.append(")");
            }

            if (!isNullOrEmpty(sDBDefault)) {
                sb.append(" DEFAULT ");
                sb.append(sDBDefault);
            }

            first = false;
        }

        if (keys.size() > 0) {
            sb.append(", CONSTRAINT pk_");
            sb.append(table);
            sb.append(" PRIMARY KEY (");

            first = true;
            for (String k : keys) {
                if (!first) {
                    sb.append(", ");
                }

                sb.append(k);

                first = false;
            }
            sb.append(")");
        }

        return sb;
    }

    private static boolean isNullOrEmpty(String s) {
        if (s == null) {
            return true;
        }

        return s.trim().isEmpty();
    }
}
