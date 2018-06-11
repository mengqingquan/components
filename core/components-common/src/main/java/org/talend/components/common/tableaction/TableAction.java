package org.talend.components.common.tableaction;

import java.sql.Connection;
import java.sql.SQLException;

public interface TableAction {

    public static enum TableActionEnum {
        NONE,
        DROP_CREATE,
        CREATE,
        CREATE_IF_NOT_EXISTS,
        DROP_IF_EXISTS_AND_CREATE,
        CLEAR,
        TRUNCATE
    }

    void exec(Connection connection) throws SQLException;

}
