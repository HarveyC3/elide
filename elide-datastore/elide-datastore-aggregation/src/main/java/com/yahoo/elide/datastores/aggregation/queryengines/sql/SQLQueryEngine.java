/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.queryengines.sql;

import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.TimedFunction;
import com.yahoo.elide.core.exceptions.InvalidPredicateException;
import com.yahoo.elide.core.filter.FilterPredicate;
import com.yahoo.elide.core.filter.expression.PredicateExtractionVisitor;
import com.yahoo.elide.datastores.aggregation.QueryEngine;
import com.yahoo.elide.datastores.aggregation.metadata.MetaDataStore;
import com.yahoo.elide.datastores.aggregation.metadata.models.*;
import com.yahoo.elide.datastores.aggregation.query.*;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.AbstractSqlQueryDialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl.DefaultDialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata.SQLMetric;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata.SQLReferenceTable;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata.SQLTable;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metric.SQLMetricFunction;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.query.*;
import com.yahoo.elide.request.Argument;
import com.yahoo.elide.request.Pagination;
import com.yahoo.elide.utils.coerce.CoerceUtil;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.jpa.QueryHints;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.*;
import java.util.stream.Collectors;

import static com.yahoo.elide.utils.TypeHelper.getTypeAlias;

/**
 * QueryEngine for SQL backed stores.
 */
@Slf4j
public class SQLQueryEngine extends QueryEngine {
    private final EntityManagerFactory entityManagerFactory;

    private final SQLReferenceTable referenceTable;
    private final AbstractSqlQueryDialect dialect;

    public SQLQueryEngine(MetaDataStore metaDataStore, EntityManagerFactory entityManagerFactory, Cache cache) {
        this(metaDataStore,entityManagerFactory,cache, new DefaultDialect());
    }
    public SQLQueryEngine(MetaDataStore metaDataStore, EntityManagerFactory entityManagerFactory, Cache cache,
                          AbstractSqlQueryDialect abstractSqlQueryDialect) {
        super(metaDataStore, cache);
        this.entityManagerFactory = entityManagerFactory;
        this.referenceTable = new SQLReferenceTable(metaDataStore);
        this.dialect = abstractSqlQueryDialect;
    }

        @Override
    protected Table constructTable(Class<?> entityClass, EntityDictionary metaDataDictionary) {
        return new SQLTable(entityClass, metaDataDictionary);
    }

    @Override
    public ColumnProjection constructDimensionProjection(Dimension dimension,
                                                         String alias,
                                                         Map<String, Argument> arguments) {
        return new SQLColumnProjection() {
            @Override
            public SQLReferenceTable getReferenceTable() {
                return referenceTable;
            }

            @Override
            public Column getColumn() {
                return dimension;
            }

            @Override
            public String getAlias() {
                return alias;
            }

            @Override
            public Map<String, Argument> getArguments() {
                return arguments;
            }
        };
    }

    @Override
    public TimeDimensionProjection constructTimeDimensionProjection(TimeDimension dimension,
                                                                    String alias,
                                                                    Map<String, Argument> arguments) {
        return new SQLTimeDimensionProjection(dimension, dimension.getTimezone(), referenceTable, alias, arguments);
    }

    @Override
    public MetricProjection constructMetricProjection(Metric metric,
                                                      String alias,
                                                      Map<String, Argument> arguments) {
        return new SQLMetricProjection(metric, referenceTable, alias, arguments);
    }

    @Override
    public Iterable<Object> executeQuery(Query query, boolean useCache) {
        EntityManager entityManager = null;
        EntityTransaction transaction = null;
        try {
            entityManager = entityManagerFactory.createEntityManager();

            // manually begin the transaction
            transaction = entityManager.getTransaction();
            if (!transaction.isActive()) {
                transaction.begin();
            }

            // Translate the query into SQL.
            SQLQuery sql = toSQL(query, this.dialect);
            log.debug("SQL Query: " + sql);

            javax.persistence.Query jpaQuery = entityManager.createNativeQuery(sql.toString());

            Pagination pagination = query.getPagination();
            if (pagination != null) {
                jpaQuery.setFirstResult(pagination.getOffset());
                jpaQuery.setMaxResults(pagination.getLimit());

                if (pagination.returnPageTotals()) {

                    SQLQuery paginationSQL = toPageTotalSQL(sql, this.dialect);
                    javax.persistence.Query pageTotalQuery =
                            entityManager.createNativeQuery(paginationSQL.toString())
                                    .setHint(QueryHints.HINT_READONLY, true);

                    //Supply the query parameters to the query
                    supplyFilterQueryParameters(query, pageTotalQuery);

                    //Run the Pagination query and log the time spent.
                    long total = new TimedFunction<>(
                            () -> CoerceUtil.coerce(pageTotalQuery.getSingleResult(), Long.class),
                            "Running Query: " + paginationSQL
                    ).get();

                    pagination.setPageTotals(total);
                }
            }

            // Supply the query parameters to the query
            supplyFilterQueryParameters(query, jpaQuery);

            // Run the primary query and log the time spent.
            List<Object> results = new TimedFunction<>(
                    () -> jpaQuery.setHint(QueryHints.HINT_READONLY, true).getResultList(),
                    "Running Query: " + sql).get();

            return new SQLEntityHydrator(results, query, getMetadataDictionary(), entityManager).hydrate();
        } finally {
            if (transaction != null && transaction.isActive()) {
                transaction.commit();
            }
            if (entityManager != null) {
                entityManager.close();
            }
        }
    }

    @Override
    public List<String> showQueries(Query query) {
        List<String> queries;
        queries = new ArrayList<String>();
        SQLQuery sql = toSQL(query, this.dialect);
        EntityManager entityManager = null;
        entityManager = entityManagerFactory.createEntityManager();
        javax.persistence.Query jpaQuery = entityManager.createNativeQuery(sql.toString());

        Pagination pagination = query.getPagination();
        if (pagination != null) {
            if (pagination.returnPageTotals()) {
                SQLQuery paginationSQL = toPageTotalSQL(sql, this.dialect);
                queries.add(paginationSQL.toString());
            }
        }
        supplyFilterQueryParameters(query, jpaQuery);
        queries.add(sql.toString());
        return queries;
    }

        /**
         * Translates the client query into SQL.
         *
         * @param query the client query.
         * @return the SQL query.
         */
    private SQLQuery toSQL(Query query, AbstractSqlQueryDialect abstractSqlQueryDialect) {
        Set<ColumnProjection> groupByDimensions = new LinkedHashSet<>(query.getGroupByDimensions());
        Set<TimeDimensionProjection> timeDimensions = new LinkedHashSet<>(query.getTimeDimensions());

        SQLQueryTemplate queryTemplate = query.getMetrics().stream()
                .map(metricProjection -> {
                    if (!(metricProjection.getColumn().getMetricFunction() instanceof SQLMetricFunction)) {
                        throw new InvalidPredicateException(
                                "Non-SQL metric function on " + metricProjection.getAlias());
                    }

                    return ((SQLMetric) metricProjection.getColumn()).resolve(query, metricProjection, referenceTable);
                })
                .reduce(SQLQueryTemplate::merge)
                .orElse(new SQLQueryTemplate(query));

        return new SQLQueryConstructor(referenceTable, abstractSqlQueryDialect).resolveTemplate(
                query,
                queryTemplate,
                query.getSorting(),
                query.getWhereFilter(),
                query.getHavingFilter());
    }


    /**
     * Given a JPA query, replaces any parameters with their values from client query.
     *
     * @param query The client query
     * @param jpaQuery The JPA query
     */
    private void supplyFilterQueryParameters(Query query,
                                             javax.persistence.Query jpaQuery) {

        Collection<FilterPredicate> predicates = new ArrayList<>();
        if (query.getWhereFilter() != null) {
            predicates.addAll(query.getWhereFilter().accept(new PredicateExtractionVisitor()));
        }

        if (query.getHavingFilter() != null) {
            predicates.addAll(query.getHavingFilter().accept(new PredicateExtractionVisitor()));
        }

        for (FilterPredicate filterPredicate : predicates) {
            if (filterPredicate.getOperator().isParameterized()) {
                boolean shouldEscape = filterPredicate.isMatchingOperator();
                filterPredicate.getParameters().forEach(param -> {
                    jpaQuery.setParameter(param.getName(), shouldEscape ? param.escapeMatching() : param.getValue());
                });
            }
        }
    }

    /**
     * Takes a SQLQuery and creates a new clone that instead returns the total number of records of the original
     * query.
     *
     * @param sql The original query
     * @return A new query that returns the total number of records.
     */
    private SQLQuery toPageTotalSQL(SQLQuery sql, AbstractSqlQueryDialect abstractSqlQueryDialect) {
        // TODO: refactor this method
        String groupByDimensions =
                extractSQLDimensions(sql.getClientQuery(), sql.getClientQuery().getTable())
                        .stream()
                        .map(dimension -> referenceTable.getResolvedReference(
                                sql.getClientQuery().getTable(),
                                dimension.getName()))
                        .collect(Collectors.joining(", "));

        String projectionClause = String.format("COUNT(DISTINCT(%s))", groupByDimensions);

        return SQLQuery.builder()
                .clientQuery(sql.getClientQuery())
                .projectionClause(projectionClause)
                .fromClause(sql.getFromClause())
                .joinClause(sql.getJoinClause())
                .whereClause(sql.getWhereClause())
                .build();
    }

    /**
     * Extract dimension projects in a query to sql dimensions.
     *
     * @param query requested query
     * @param table queried table
     * @return sql dimensions in this query
     */
    private List<Dimension> extractSQLDimensions(Query query, Table table) {
        return query.getDimensions().stream()
                .map(projection -> table.getDimension(projection.getColumn().getName()))
                .collect(Collectors.toList());
    }

    /**
     * Get alias for an entity class.
     *
     * @param entityClass entity class
     * @return alias
     */
    public static String getClassAlias(Class<?> entityClass) {
        return getTypeAlias(entityClass);
    }
}
