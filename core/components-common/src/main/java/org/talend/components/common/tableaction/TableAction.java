package org.talend.components.common.tableaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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

    /**
     *
     * @return List<String> List of queries to execute.
     */
    List<String> getQueries() throws Exception;

}
