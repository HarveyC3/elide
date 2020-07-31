package com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl;

import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.AbstractSqlQueryDialect;

public class PrestoDialect extends AbstractSqlQueryDialect {
    @Override
    public String getDialectType() {
        return "Presto";
    }
}
