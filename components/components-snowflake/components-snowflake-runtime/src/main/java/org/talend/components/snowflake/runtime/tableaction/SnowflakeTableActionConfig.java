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
package org.talend.components.snowflake.runtime.tableaction;

import org.talend.components.common.tableaction.TableActionConfig;

public class SnowflakeTableActionConfig extends TableActionConfig {

    public SnowflakeTableActionConfig(boolean isUpperCase){
        this.SQL_UPPERCASE_IDENTIFIER = isUpperCase;
        this.SQL_ESCAPE_ENABLED = true;
        this.SQL_DROP_TABLE_SUFFIX = " CASCADE";
    }

}
