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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public class DefaultSQLTruncateTableAction extends TableAction {

    private final Logger log = LoggerFactory.getLogger(DefaultSQLTruncateTableAction.class);

    private String table;

    public DefaultSQLTruncateTableAction(final String table) {
        if (table == null || table.isEmpty()) {
            throw new InvalidParameterException("Table name can't null or empty");
        }

        this.table = table;

    }

    @Override
    public List<String> getQueries() throws Exception {
        List<String> queries = new ArrayList<>();

        queries.add(getTruncateTableQuery());

        if (log.isDebugEnabled()) {
            log.debug("Generated SQL queries to truncate table:");
            for (String q : queries) {
                log.debug(q);
            }
        }

        return queries;
    }

    private String getTruncateTableQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getConfig().SQL_TRUNCATE_PREFIX);
        sb.append(this.getConfig().SQL_TRUNCATE);
        sb.append(" ");
        sb.append(escape(table));
        sb.append(this.getConfig().SQL_TRUNCATE_SUFFIX);
        return sb.toString();
    }

}
