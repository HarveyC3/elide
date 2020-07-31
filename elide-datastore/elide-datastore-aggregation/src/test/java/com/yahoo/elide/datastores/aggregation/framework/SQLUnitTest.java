/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.framework;

import com.google.common.collect.Lists;
import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.Path;
import com.yahoo.elide.core.filter.FilterPredicate;
import com.yahoo.elide.core.filter.Operator;
import com.yahoo.elide.core.filter.dialect.RSQLFilterDialect;
import com.yahoo.elide.core.filter.expression.AndFilterExpression;
import com.yahoo.elide.core.filter.expression.OrFilterExpression;
import com.yahoo.elide.core.pagination.PaginationImpl;
import com.yahoo.elide.core.sort.SortingImpl;
import com.yahoo.elide.datastores.aggregation.QueryEngine;
import com.yahoo.elide.datastores.aggregation.example.Continent;
import com.yahoo.elide.datastores.aggregation.example.Country;
import com.yahoo.elide.datastores.aggregation.example.CountryView;
import com.yahoo.elide.datastores.aggregation.example.CountryViewNested;
import com.yahoo.elide.datastores.aggregation.example.Player;
import com.yahoo.elide.datastores.aggregation.example.PlayerStats;
import com.yahoo.elide.datastores.aggregation.example.PlayerStatsView;
import com.yahoo.elide.datastores.aggregation.example.PlayerStatsWithView;
import com.yahoo.elide.datastores.aggregation.example.SubCountry;
import com.yahoo.elide.datastores.aggregation.metadata.MetaDataStore;
import com.yahoo.elide.datastores.aggregation.metadata.enums.TimeGrain;
import com.yahoo.elide.datastores.aggregation.metadata.models.Dimension;
import com.yahoo.elide.datastores.aggregation.metadata.models.Metric;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.metadata.models.TimeDimension;
import com.yahoo.elide.datastores.aggregation.query.ColumnProjection;
import com.yahoo.elide.datastores.aggregation.query.MetricProjection;
import com.yahoo.elide.datastores.aggregation.query.Query;
import com.yahoo.elide.datastores.aggregation.query.TimeDimensionProjection;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialectFactory;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialect;
import com.yahoo.elide.request.Argument;
import com.yahoo.elide.request.Sorting;
import com.yahoo.elide.utils.ClassScanner;

import java.util.*;
import java.util.regex.Pattern;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class SQLUnitTest {
    protected static EntityManagerFactory emf;
    protected static Table playerStatsTable;
    protected static EntityDictionary dictionary;
    protected static RSQLFilterDialect filterParser;
    protected static MetaDataStore metaDataStore;

    protected static final Country HONG_KONG = new Country();
    protected static final Country USA = new Country();
    protected static final Continent ASIA = new Continent();
    protected static final Continent NA = new Continent();

    protected static QueryEngine engine;

    // Standard set of test queries used in dialect tests
    protected enum TestQueryName {
        WHERE_METRICS_ONLY,
        WHERE_DIMS_ONLY,
        WHERE_METRICS_AND_DIMS,
        WHERE_METRICS_OR_DIMS,
        HAVING_METRICS_ONLY,
        HAVING_DIMS_ONLY,
        HAVING_METRICS_AND_DIMS,
        HAVING_METRICS_OR_DIMS,
        PAGINATION_TOTAL,
        PAGINATION_PAGE_AND_TOTAL,
        SORT_METRIC_ASC,
        SORT_METRIC_DESC,
        SORT_DIM_DESC,
        SORT_METRIC_AND_DIM_DESC,
        SUBQUERY,
        GROUP_BY_METRIC_NOT_IN_SELECT,
        UDF_IN_GROUP_BY,
        COMPLICATED
    }
    protected static Map<TestQueryName, Query> testQueries;

    public static void init(SQLDialect sqlDialect) {
        metaDataStore = new MetaDataStore(ClassScanner.getAllClasses("com.yahoo.elide.datastores.aggregation.example"));

        emf = Persistence.createEntityManagerFactory("aggregationStore");
        dictionary = new EntityDictionary(new HashMap<>());
        dictionary.bindEntity(PlayerStatsWithView.class);
        dictionary.bindEntity(PlayerStatsView.class);
        dictionary.bindEntity(PlayerStats.class);
        dictionary.bindEntity(Country.class);
        dictionary.bindEntity(SubCountry.class);
        dictionary.bindEntity(Player.class);
        dictionary.bindEntity(CountryView.class);
        dictionary.bindEntity(CountryViewNested.class);
        dictionary.bindEntity(Continent.class);
        filterParser = new RSQLFilterDialect(dictionary);

        metaDataStore.populateEntityDictionary(dictionary);

        engine = new SQLQueryEngine(metaDataStore, emf, null, sqlDialect);
        playerStatsTable = engine.getTable("playerStats");

        ASIA.setName("Asia");
        ASIA.setId("1");

        NA.setName("North America");
        NA.setId("2");

        HONG_KONG.setIsoCode("HKG");
        HONG_KONG.setName("Hong Kong");
        HONG_KONG.setId("344");
        HONG_KONG.setContinent(ASIA);

        USA.setIsoCode("USA");
        USA.setName("United States");
        USA.setId("840");
        USA.setContinent(NA);

        buildTestQueryMap();
    }

    public static void init(){
        init(new SQLDialectFactory().getDefaultDialect());
    }

    public static ColumnProjection toProjection(Dimension dimension) {
        return engine.constructDimensionProjection(dimension, dimension.getName(), Collections.emptyMap());
    }

    public static TimeDimensionProjection toProjection(TimeDimension dimension, TimeGrain grain) {
        return engine.constructTimeDimensionProjection(
                dimension,
                dimension.getName(),
                Collections.singletonMap("grain", Argument.builder().name("grain").value(grain).build()));
    }

    public static MetricProjection invoke(Metric metric) {
        return engine.constructMetricProjection(metric, metric.getName(), Collections.emptyMap());
    }

    /**
     * All functions below are helpers to simplify SQL showQuery tests.
     */
    /**
     * Automatically convert a single expected string into a List with one element
     * @param expected
     * @param actual
     */
    protected void compareQueryLists(String expected, List<String> actual) {
        compareQueryLists(Arrays.asList(expected), actual);
    }

    /**
     * Helper for comparing lists of queries.
     */
    protected void compareQueryLists(List<String> expected, List<String> actual) {
        if (expected == null && actual == null) {
            return;
        } else if (expected == null) {
            fail("Expected a null query List, but actual was non-null");
        } else if (actual == null) {
            fail("Expected a non-null query List, but actual was null");
        }
        assertEquals(expected.size(), actual.size(), "Query List sizes do not match");
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(combineWhitespace(expected.get(i).trim()), combineWhitespace(actual.get(i).trim()));
        }
    }

    /**
     * Helper to remove repeated whitespace chars before comparing queries
     */
    protected Pattern repeatedWhitespacePattern = Pattern.compile("\\s\\s*");
    protected String combineWhitespace(String input) {
        return repeatedWhitespacePattern.matcher(input).replaceAll(" ");
    }

    /**
     * Helper to build the standard test Query objects used by the dialect tests.
     * @throws Exception
     */
    private static void buildTestQueryMap() {
        testQueries = new EnumMap<TestQueryName, Query>(TestQueryName.class);
        {
            FilterPredicate predicate = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScoreNoAgg"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query whereMetricsOnlyQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScoreNoAgg")))
                    .metric(invoke(playerStatsTable.getMetric("lowScore")))
                    .whereFilter(predicate)
                    .build();
            testQueries.put(TestQueryName.WHERE_METRICS_ONLY, whereMetricsOnlyQuery);
        }
        {
            FilterPredicate predicate = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            Query whereDimsOnlyQuery = Query.builder()
                    .table(playerStatsTable)
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .whereFilter(predicate)
                    .build();
            testQueries.put(TestQueryName.WHERE_DIMS_ONLY, whereDimsOnlyQuery);
        }
        {
            FilterPredicate ratingFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            FilterPredicate highScoreFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScore"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query whereMetricsAndDimsQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .whereFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                    .build();
            testQueries.put(TestQueryName.WHERE_METRICS_AND_DIMS, whereMetricsAndDimsQuery);
        }
        {
            FilterPredicate ratingFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            FilterPredicate highScoreFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScore"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query whereMetricsOrDimsQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .whereFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                    .build();
            testQueries.put(TestQueryName.WHERE_METRICS_OR_DIMS, whereMetricsOrDimsQuery);
        }
        {
            FilterPredicate predicate = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScoreNoAgg"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query havingMetricsOnlyQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScoreNoAgg")))
                    .metric(invoke(playerStatsTable.getMetric("lowScore")))
                    .havingFilter(predicate)
                    .build();
            testQueries.put(TestQueryName.HAVING_METRICS_ONLY, havingMetricsOnlyQuery);
        }
        {
            FilterPredicate predicate = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            Query havingDimsOnlyQuery = Query.builder()
                    .table(playerStatsTable)
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .havingFilter(predicate)
                    .build();
            testQueries.put(TestQueryName.HAVING_DIMS_ONLY, havingDimsOnlyQuery);
        }
        {
            FilterPredicate ratingFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            FilterPredicate highScoreFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScore"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query havingMetricsAndDimsQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .havingFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                    .build();
            testQueries.put(TestQueryName.HAVING_METRICS_AND_DIMS, havingMetricsAndDimsQuery);
        }
        {
            FilterPredicate ratingFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "overallRating"),
                    Operator.NOTNULL, new ArrayList<Object>());
            FilterPredicate highScoreFilter = new FilterPredicate(
                    new Path(PlayerStats.class, dictionary, "highScore"),
                    Operator.GT,
                    Lists.newArrayList(9000));
            Query havingMetricsOrDimsQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .havingFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                    .build();
            testQueries.put(TestQueryName.HAVING_METRICS_OR_DIMS, havingMetricsOrDimsQuery);
        }
        {
            PaginationImpl pagination = new PaginationImpl(
                    PlayerStats.class,
                    0,
                    1,
                    PaginationImpl.DEFAULT_PAGE_LIMIT,
                    PaginationImpl.MAX_PAGE_LIMIT,
                    true,
                    false
            );
            Query paginationQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("lowScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                    .pagination(pagination)
                    .build();
            testQueries.put(TestQueryName.PAGINATION_TOTAL, paginationQuery);
        }
        {
            Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
            sortMap.put("highScoreNoAgg", Sorting.SortOrder.asc);
            Query sortMetricAscQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScoreNoAgg")))
                    .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                    .build();
            testQueries.put(TestQueryName.SORT_METRIC_ASC, sortMetricAscQuery);
        }
        {
            Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
            sortMap.put("highScoreNoAgg", Sorting.SortOrder.desc);
            Query sortMetricDescQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScoreNoAgg")))
                    .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                    .build();
            testQueries.put(TestQueryName.SORT_METRIC_DESC, sortMetricDescQuery);
        }
        {
            Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
            sortMap.put("overallRating", Sorting.SortOrder.desc);
            Query sortDimDescQuery = Query.builder()
                    .table(playerStatsTable)
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                    .build();
            testQueries.put(TestQueryName.SORT_DIM_DESC, sortDimDescQuery);
        }
        {
            Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
            sortMap.put("highScore", Sorting.SortOrder.desc);
            sortMap.put("overallRating", Sorting.SortOrder.desc);
            Query sortMetricAndDimQuery = Query.builder()
                    .table(playerStatsTable)
                    .metric(invoke(playerStatsTable.getMetric("highScore")))
                    .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                    .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                    .build();
            testQueries.put(TestQueryName.SORT_METRIC_AND_DIM_DESC, sortMetricAndDimQuery);
        }
        {
            Table playerStatsViewTable = engine.getTable("playerStatsView");
            Query selectFromSubquery = Query.builder()
                    .table(playerStatsViewTable)
                    .metric(invoke(playerStatsViewTable.getMetric("highScore")))
                    .build();
            testQueries.put(TestQueryName.SUBQUERY, selectFromSubquery);
        }
    }

}
