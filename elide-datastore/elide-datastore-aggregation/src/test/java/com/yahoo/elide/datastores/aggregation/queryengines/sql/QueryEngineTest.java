/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.queryengines.sql;

import com.google.common.collect.Lists;
import com.yahoo.elide.core.Path;
import com.yahoo.elide.core.filter.FilterPredicate;
import com.yahoo.elide.core.filter.Operator;
import com.yahoo.elide.core.filter.expression.AndFilterExpression;
import com.yahoo.elide.core.filter.expression.OrFilterExpression;
import com.yahoo.elide.core.pagination.PaginationImpl;
import com.yahoo.elide.core.sort.SortingImpl;
import com.yahoo.elide.datastores.aggregation.example.PlayerStats;
import com.yahoo.elide.datastores.aggregation.example.PlayerStatsView;
import com.yahoo.elide.datastores.aggregation.framework.SQLUnitTest;
import com.yahoo.elide.datastores.aggregation.metadata.enums.TimeGrain;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.query.Query;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.annotation.FromSubquery;
import com.yahoo.elide.request.Sorting;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class QueryEngineTest extends SQLUnitTest {
    private static Table playerStatsViewTable;

    @BeforeAll
    public static void init() {
        SQLUnitTest.init();

        playerStatsViewTable = engine.getTable("playerStatsView");
    }

    /**
     * Test loading all three records from the table.
     */
    @Test
    public void testFullTableLoad() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setLowScore(241);
        stats0.setHighScore(2412);
        stats0.setRecordedDate(Timestamp.valueOf("2019-07-11 00:00:00"));

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setLowScore(35);
        stats1.setHighScore(1234);
        stats1.setRecordedDate(Timestamp.valueOf("2019-07-12 00:00:00"));

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("2");
        stats2.setLowScore(72);
        stats2.setHighScore(1000);
        stats2.setRecordedDate(Timestamp.valueOf("2019-07-13 00:00:00"));

        assertEquals(3, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
        assertEquals(stats2, results.get(2));
    }

    /**
     * Test loading records using {@link FromSubquery}
     */
    @Test
    public void testFromSubQuery() {
        Query query = Query.builder()
                .table(playerStatsViewTable)
                .metric(invoke(playerStatsViewTable.getMetric("highScore")))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStatsView stats2 = new PlayerStatsView();
        stats2.setId("0");
        stats2.setHighScore(2412);

        assertEquals(1, results.size());
        assertEquals(stats2, results.get(0));
    }

    /**
     * Test group by, having, dimension, metric at the same time.
     *
     * @throws Exception exception
     */
    @Test
    public void testAllArgumentQuery() throws Exception {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("countryName", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsViewTable)
                .metric(invoke(playerStatsViewTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsViewTable.getDimension("countryName")))
                .whereFilter(filterParser.parseFilterExpression("countryName=='United States'",
                        PlayerStatsView.class, false))
                .havingFilter(filterParser.parseFilterExpression("highScore > 300",
                        PlayerStatsView.class, false))
                .sorting(new SortingImpl(sortMap, PlayerStatsView.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStatsView stats2 = new PlayerStatsView();
        stats2.setId("0");
        stats2.setHighScore(2412);
        stats2.setCountryName("United States");

        assertEquals(1, results.size());
        assertEquals(stats2, results.get(0));
    }

    /**
     * Test group by a degenerate dimension with a filter applied.
     *
     * @throws Exception exception
     */
    @Test
    public void testDegenerateDimensionFilter() throws Exception {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .whereFilter(filterParser.parseFilterExpression("overallRating==Great",
                        PlayerStats.class, false))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setLowScore(241);
        stats1.setOverallRating("Great");
        stats1.setRecordedDate(Timestamp.valueOf("2019-07-11 00:00:00"));

        assertEquals(1, results.size());
        assertEquals(stats1, results.get(0));
    }

    /**
     * Test filtering on an attribute that's not present in the query.
     *
     * @throws Exception exception
     */
    @Test
    public void testNotProjectedFilter() throws Exception {
        Query query = Query.builder()
                .table(playerStatsViewTable)
                .metric(invoke(playerStatsViewTable.getMetric("highScore")))
                .whereFilter(filterParser.parseFilterExpression("countryName=='United States'",
                        PlayerStatsView.class, false))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStatsView stats2 = new PlayerStatsView();
        stats2.setId("0");
        stats2.setHighScore(2412);

        assertEquals(1, results.size());
        assertEquals(stats2, results.get(0));
    }

    @Test
    public void testSortAggregatedMetric() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setLowScore(241);
        stats0.setOverallRating("Great");

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setLowScore(35);
        stats1.setOverallRating("Good");

        assertEquals(2, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
    }

    /**
     * Test sorting by dimension attribute which is not present in the query.
     */
    @Test
    public void testSortJoin() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("playerName", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setLowScore(72);
        stats0.setOverallRating("Good");
        stats0.setRecordedDate(Timestamp.valueOf("2019-07-13 00:00:00"));

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setLowScore(241);
        stats1.setOverallRating("Great");
        stats1.setRecordedDate(Timestamp.valueOf("2019-07-11 00:00:00"));

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("2");
        stats2.setLowScore(35);
        stats2.setOverallRating("Good");
        stats2.setRecordedDate(Timestamp.valueOf("2019-07-12 00:00:00"));

        assertEquals(3, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
        assertEquals(stats2, results.get(2));
    }

    /**
     * Test pagination.
     */
    @Test
    public void testPagination() {
        PaginationImpl pagination = new PaginationImpl(
                PlayerStats.class,
                0,
                1,
                PaginationImpl.DEFAULT_PAGE_LIMIT,
                PaginationImpl.MAX_PAGE_LIMIT,
                true,
                false
        );

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .pagination(pagination)
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        //Jon Doe,1234,72,Good,840,2019-07-12 00:00:00
        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setLowScore(35);
        stats1.setOverallRating("Good");
        stats1.setRecordedDate(Timestamp.valueOf("2019-07-12 00:00:00"));

        assertEquals(results.size(), 1, "Number of records returned does not match");
        assertEquals(results.get(0), stats1, "Returned record does not match");
        assertEquals(pagination.getPageTotals(), 3, "Page totals does not match");
    }

    /**
     * Test having clause integrates with group by clause.
     *
     * @throws Exception exception
     */
    @Test
    public void testHavingClause() throws Exception {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(filterParser.parseFilterExpression("highScore < 2400",
                        PlayerStats.class, false))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        // Only "Good" rating would have total high score less than 2400
        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setOverallRating("Good");
        stats1.setHighScore(1234);

        assertEquals(1, results.size());
        assertEquals(stats1, results.get(0));
    }

    /**
     * Test having clause integrates with group by clause and join.
     *
     * @throws Exception exception
     */
    @Test
    public void testHavingClauseJoin() throws Exception {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryIsoCode")))
                .havingFilter(filterParser.parseFilterExpression("countryIsoCode==USA",
                        PlayerStats.class, false))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setOverallRating("Great");
        stats0.setCountryIsoCode("USA");
        stats0.setHighScore(2412);

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setOverallRating("Good");
        stats1.setCountryIsoCode("USA");
        stats1.setHighScore(1234);

        assertEquals(2, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
    }

    /**
     * Test sorting by two different columns-one metric and one dimension.
     */
    @Test
    public void testSortByMultipleColumns() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);
        sortMap.put("playerName", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setLowScore(241);
        stats0.setOverallRating("Great");
        stats0.setRecordedDate(Timestamp.valueOf("2019-07-11 00:00:00"));

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setLowScore(72);
        stats1.setOverallRating("Good");
        stats1.setRecordedDate(Timestamp.valueOf("2019-07-13 00:00:00"));

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("2");
        stats2.setLowScore(35);
        stats2.setOverallRating("Good");
        stats2.setRecordedDate(Timestamp.valueOf("2019-07-12 00:00:00"));

        assertEquals(3, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
        assertEquals(stats2, results.get(2));
    }

    /**
     * Test grouping by a dimension with a JoinTo annotation.
     */
    @Test
    public void testJoinToGroupBy() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryIsoCode")))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setHighScore(2412);
        stats1.setCountryIsoCode("USA");

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("1");
        stats2.setHighScore(1000);
        stats2.setCountryIsoCode("HKG");

        assertEquals(2, results.size());
        assertEquals(stats1, results.get(0));
        assertEquals(stats2, results.get(1));
    }

    /**
     * Test grouping by a dimension with a JoinTo annotation.
     *
     * @throws Exception exception
     */
    @Test
    public void testJoinToFilter() throws Exception {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(filterParser.parseFilterExpression("countryIsoCode==USA",
                        PlayerStats.class, false))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setOverallRating("Good");
        stats1.setHighScore(1234);

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("1");
        stats2.setOverallRating("Great");
        stats2.setHighScore(2412);

        assertEquals(2, results.size());
        assertEquals(stats1, results.get(0));
        assertEquals(stats2, results.get(1));
    }

    /**
     * Test grouping by a dimension with a JoinTo annotation.
     */
    @Test
    public void testJoinToSort() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("countryIsoCode", Sorting.SortOrder.asc);
        sortMap.put("highScore", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryIsoCode")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setOverallRating("Good");
        stats1.setCountryIsoCode("HKG");
        stats1.setHighScore(1000);

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("1");
        stats2.setOverallRating("Good");
        stats2.setCountryIsoCode("USA");
        stats2.setHighScore(1234);

        PlayerStats stats3 = new PlayerStats();
        stats3.setId("2");
        stats3.setOverallRating("Great");
        stats3.setCountryIsoCode("USA");
        stats3.setHighScore(2412);

        assertEquals(3, results.size());
        assertEquals(stats1, results.get(0));
        assertEquals(stats2, results.get(1));
        assertEquals(stats3, results.get(2));
    }

    /**
     * Test month grain query.
     */
    @Test
    public void testTotalScoreByMonth() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.MONTH))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setHighScore(2412);
        stats0.setRecordedDate(Timestamp.valueOf("2019-07-01 00:00:00"));

        assertEquals(1, results.size());
        assertEquals(stats0, results.get(0));
    }

    /**
     * Test filter by time dimension.
     */
    @Test
    public void testFilterByTemporalDimension() {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "recordedDate"),
                Operator.IN,
                Lists.newArrayList(Timestamp.valueOf("2019-07-11 00:00:00")));

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
                .whereFilter(predicate)
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setHighScore(2412);
        stats0.setRecordedDate(Timestamp.valueOf("2019-07-11 00:00:00"));

        assertEquals(1, results.size());
        assertEquals(stats0, results.get(0));
    }

    @Test
    public void testAmbiguousFields() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("playerName")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("player2Name")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats0 = new PlayerStats();
        stats0.setId("0");
        stats0.setLowScore(35);
        stats0.setPlayerName("Jon Doe");
        stats0.setPlayer2Name("Jane Doe");

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("1");
        stats1.setLowScore(72);
        stats1.setPlayerName("Han");
        stats1.setPlayer2Name("Jon Doe");

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("2");
        stats2.setLowScore(241);
        stats2.setPlayerName("Jane Doe");
        stats2.setPlayer2Name("Han");

        assertEquals(3, results.size());
        assertEquals(stats0, results.get(0));
        assertEquals(stats1, results.get(1));
        assertEquals(stats2, results.get(2));
    }

    @Test
    public void testNullJoinToStringValue() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryNickName")))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setHighScore(2412);
        stats1.setCountryNickName("Uncle Sam");

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("1");
        stats2.setHighScore(1000);
        stats2.setCountryNickName(null);

        assertEquals(2, results.size());
        assertEquals(stats1, results.get(0));
        assertEquals(stats2, results.get(1));
    }

    @Test
    public void testNullJoinToIntValue() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryUnSeats")))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStats stats1 = new PlayerStats();
        stats1.setId("0");
        stats1.setHighScore(2412);
        stats1.setCountryUnSeats(1);

        PlayerStats stats2 = new PlayerStats();
        stats2.setId("1");
        stats2.setHighScore(1000);
        stats2.setCountryUnSeats(0);

        assertEquals(2, results.size());
        assertEquals(stats1, results.get(0));
        assertEquals(stats2, results.get(1));
    }

    @Test
    public void testShowQueryNoMetricsOrDimensions() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .build();
        String expectedQueryStr = "SELECT DISTINCT  " +
                "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesWhereMetricsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .whereFilter(predicate)
                .build();
        List<FilterPredicate.FilterParameter> params = predicate.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        + "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder();
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesWhereDimsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(predicate)
                .build();

        String expectedQueryStr =
                "SELECT DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesWhereMetricsAndDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "AND MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder()
                        + ") GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesWhereMetricsOrDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "OR MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder()
                        + ") GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .havingFilter(predicate)
                .build();
        List<FilterPredicate.FilterParameter> params = predicate.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        + "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "HAVING MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder();
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingDimsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(predicate)
                .build();

        String expectedQueryStr =
                "SELECT DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "HAVING com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsAndDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating "
                        + "HAVING (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "AND MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder() + ")";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsOrDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "highScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating "
                        + "HAVING (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "OR MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder() + ")";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }



    @Test
    public void testShowQueriesPagination() {
//         PaginationImpl pagination = new PaginationImpl(
//                        PlayerStats.class,
//                        0,
//                        1,
//                        PaginationImpl.DEFAULT_PAGE_LIMIT,
//                        PaginationImpl.MAX_PAGE_LIMIT,
//                        true,
//                        false
//                );
//
//                Query query = Query.builder()
//                        .table(playerStatsTable)
//                        .metric(invoke(playerStatsTable.getMetric("lowScore")))
//                        .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
//                        .timeDimension(toProjection(playerStatsTable.getTimeDimension("recordedDate"), TimeGrain.DAY))
//                        .pagination(pagination)
//                        .build();
    }

    // Helper for comparing lists of queries.
    private void compareQueryLists(List<String> expected, List<String> actual) {
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

    // Automatically convert a single expected string into a List with one element
    private void compareQueryLists(String expected, List<String> actual) {
        compareQueryLists(Arrays.asList(expected), actual);
    }

    // Helper to remove repeated whitespace chars before comparing queries
    private Pattern repeatedWhitespacePattern = Pattern.compile("\\s\\s*");
    private String combineWhitespace(String input) {
        return repeatedWhitespacePattern.matcher(input).replaceAll(" ");
    }
        /*
    @Test
    public void testShowQueriesDebug() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("countryUnSeats")))
                .build();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        + "com_yahoo_elide_datastores_aggregation_example_PlayerStats_country.un_seats AS countryUnSeats "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "LEFT JOIN countries AS com_yahoo_elide_datastores_aggregation_example_PlayerStats_country "
                        + "ON com_yahoo_elide_datastores_aggregation_example_PlayerStats.country_id "
                        + "= com_yahoo_elide_datastores_aggregation_example_PlayerStats_country.id  "
                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats_country.un_seats";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }
 */
    // TODO: Should this be an error?
//    @Test
//    public void testShowQueryNoMetricsOrDimensions() {
//        Query query = Query.builder()
//                .table(playerStatsTable)
//                .build();
//        String expectedQueryStr = "SELECT DISTINCT  " +
//                "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats";
//        assertEquals(expectedQueryStr, engine.showQuery(query).trim());
//    }

    @Test
    public void testShowQuerySortingAscending(){
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.asc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        String expectedQueryStr =
                "SELECT MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS " +
                        "lowScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats   " +
                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating   " +
                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) ASC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));

    }

    @Test
    public void testShowQuerySortingDecending(){
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        String expectedQueryStr =
                "SELECT MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS " +
                        "lowScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats   " +
                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating   " +
                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) DESC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }

    @Test
    public void testShowQuerySortingByDimensionDesc(){
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        String expectedQueryStr =
                "SELECT DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats      " +
                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) DESC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }

    @Test
    public void testShowQuerySortingByMetricAndDimension(){
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS " +
                        "highScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating,com_yahoo_elide_datastores_aggregation_example_PlayerStats_country.iso_code AS " +
                        "countryIsoCode FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats " +
                        "LEFT JOIN countries AS com_yahoo_elide_datastores_aggregation_example_PlayerStats_country ON " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.country_id = " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats_country.id  GROUP BY " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating, " +
                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) DESC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));

    }

    @Test
    public void testShowQuerySelectFromSubquery(){

//        Query query = Query.builder()
//                .table(playerStatsViewTable)
//                .metric(invoke(playerStatsViewTable.getMetric("highScore")))
//                .build();
//
//        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
//                .collect(Collectors.toList());
//
//        PlayerStatsView stats2 = new PlayerStatsView();
//        stats2.setId("0");
//        stats2.setHighScore(2412);
//
//        assertEquals(1, results.size());
//        assertEquals(stats2, results.get(0));

=======
}
