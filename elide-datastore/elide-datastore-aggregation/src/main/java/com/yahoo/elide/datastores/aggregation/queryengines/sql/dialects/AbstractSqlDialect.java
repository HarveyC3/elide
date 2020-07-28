package com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects;

/**
 * All known differences between dialects will be listed here. Dialects that differ from the default dialect (H2) should
 * have their own implementation of the relalvent methods.
 */
public abstract class AbstractSqlDialect implements SQLDialect {

    public String generateCountDistinctClause(String dimensions){
        return String.format("COUNT(DISTINCT(%s))", dimensions);
    }
}
