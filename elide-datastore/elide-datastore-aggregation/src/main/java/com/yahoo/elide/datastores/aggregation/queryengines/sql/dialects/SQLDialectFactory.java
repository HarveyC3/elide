package com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects;

import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl.H2Dialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl.HiveDialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl.PrestoDialect;

public class SQLDialectFactory {

    public SQLDialectFactory(){}

    public SQLDialect getDefaultDialect(){ return new H2Dialect();}
    public SQLDialect getH2Dialect(){ return new H2Dialect();}
    public SQLDialect getHiveDialect(){ return new HiveDialect();}
    public SQLDialect getPrestoDialect(){ return new PrestoDialect();}
}
