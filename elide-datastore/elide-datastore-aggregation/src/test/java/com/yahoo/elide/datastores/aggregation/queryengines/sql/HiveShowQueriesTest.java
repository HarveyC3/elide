package com.yahoo.elide.datastores.aggregation.queryengines.sql;

import com.google.common.collect.Lists;
import com.yahoo.elide.core.Path;
import com.yahoo.elide.core.filter.FilterPredicate;
import com.yahoo.elide.core.filter.Operator;
import com.yahoo.elide.core.pagination.PaginationImpl;
import com.yahoo.elide.core.sort.SortingImpl;
import com.yahoo.elide.datastores.aggregation.example.PlayerStats;
import com.yahoo.elide.datastores.aggregation.framework.SQLUnitTest;
import com.yahoo.elide.datastores.aggregation.metadata.enums.TimeGrain;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.query.Query;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialectFactory;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.impl.HiveDialect;
import com.yahoo.elide.request.Sorting;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This class tests a Hive SQL generation of the engine.
 * This class only covers known differences/ use cases and will continue to grow as relavent differences are discovered.
 *
 * *** KEY ASSUMPTIONS ***
 *      * `from_unixtime(unix_timestamp())` shall be used instead of `PARSEDATETIME(FORMATDATETIME())`
 *           when defining a datastore a real Hive environment.
 *        - PlayerStats.DAY_FORMAT provides an example of where this logic would have to be updated
 *        - com/yahoo/elide/datastores/aggregation/example/PlayerStats.java
 *
 *      * UDAFs (User-defined Aggregation Functions) such as MIN and MAX will be supported in the Hive environment
 * *** * * * * * * * * ***
 *
 */
public class HiveShowQueriesTest extends SQLUnitTest{
    private static Table playerStatsViewTable;

    @BeforeAll
    public static void init() {
        SQLUnitTest.init(SQLDialectFactory.getHiveDialect());

        playerStatsViewTable = engine.getTable("playerStatsView");
    }

    /**
     * Where clause cannot contain aggregations.
     * If a metric/dimension alias exists in the SELECT clause:
     *     use it in the WHERE clause.
     * Else:
     *     add it to the SELECT clause then use the alias in the WHERE clause.
     * @throws Exception
     */
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
                        + "WHERE highScore > "
                        + params.get(0).getPlaceholder();
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    /**
     * This test validates that generateCountDistinctClause() is called in the HiveDialect. The difference between this
     * and the default dialect is an ommitted parentheses on the inner DISTINCT clause. (expectedQueryStr1)
     */
    @Test
    public void testShowQueriesPagination() {
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
        String expectedQueryStr1 =
                "SELECT COUNT(DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating, " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate) FROM " +
                        "playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats     ";
        String expectedQueryStr2 =
                "SELECT MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS " +
                        "lowScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating,PARSEDATETIME(FORMATDATETIME(" +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate, 'yyyy-MM-dd'), " +
                        "'yyyy-MM-dd') AS recordedDate FROM playerStats AS " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats   " +
                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating, " +
                        "PARSEDATETIME(FORMATDATETIME(" +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate, 'yyyy-MM-dd'), " +
                        "'yyyy-MM-dd')  ";
        List<String> expectedQueryList = new ArrayList<String>();
        expectedQueryList.add(expectedQueryStr1);
        expectedQueryList.add(expectedQueryStr2);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }

    /**
     * TODO
     * This is an invalid Hive query, there should be a GROUP BY $metric for any metric in the HAVING clause
     * @throws Exception
     */
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
//                 suggested method call to implement
//                .groupByMetric(playerStatsTable.getMetric("highScore"))
                .havingFilter(predicate)
                .build();
        List<FilterPredicate.FilterParameter> params = predicate.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        + "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore "
                        + "HAVING MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) > "
                        + params.get(0).getPlaceholder();
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    /**
     * TODO
     * Hive only allows ORDER BY on columns/ fields that have been selected
     * (similar to what is occurring with the overallRating dimension)
     */
    @Test
    public void testShowQuerySortingByMetricAndDimension() {
        Map<String, Sorting.SortOrder> sortMap = new TreeMap<>();
        sortMap.put("lowScore", Sorting.SortOrder.desc);

        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("highScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .sorting(new SortingImpl(sortMap, PlayerStats.class, dictionary))
                .build();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore," +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating," +
                        "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore " +
                        "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats " +
                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating " +
                        "ORDER BY lowScore DESC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }

    /**
     *
     * TODO:
     * Metrics/Dimensions in the ORDER BY clause must be in the SELECT clause first
     */
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
                "SELECT DISTINCT " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating, " +
                        "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) as lowScore " +
                        "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats " +
                        "ORDER BY lowScore DESC";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));

    }

}
