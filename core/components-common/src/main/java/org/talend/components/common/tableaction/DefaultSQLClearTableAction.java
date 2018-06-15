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

public class DefaultSQLClearTableAction implements TableAction {

    private final Logger log = LoggerFactory.getLogger(DefaultSQLClearTableAction.class);

    private String table;

    public DefaultSQLClearTableAction(final String table) {
        if (table == null || table.isEmpty()) {
            throw new InvalidParameterException("Table name can't null or empty");
        }

        this.table = table;
    }

    @Override
    public List<String> getQueries() throws Exception {
        List<String> queries = new ArrayList<>();

        queries.add(getClearTableQuery());

        if (log.isDebugEnabled()) {
            log.debug("Generated SQL queries to clear table:");
            for (String q : queries) {
                log.debug(q);
            }
        }

        return queries;
    }

    private String getClearTableQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(table);

        return sb.toString();
    }

}
