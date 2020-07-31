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
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class HiveShowQueriesTest extends SQLUnitTest{
    private static Table playerStatsViewTable;

    @BeforeAll
    public static void init() {
<<<<<<< HEAD
        SQLUnitTest.init(SQLDialectFactory.getHiveDialect());
=======
        SQLUnitTest.init();
>>>>>>> parent of 24dd8a57... add dialects and tests for expected functionality

        playerStatsViewTable = engine.getTable("playerStatsView");
    }

    @Test
    public void testShowQueryNoMetricsOrDimensions() {
        Query query = Query.builder()
                .table(playerStatsTable)
                .build();
//        String expectedQueryStr = "SELECT DISTINCT  " +
//                "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats";
        String expectedQueryStr = "Manual failing - double check invalid SQL";

        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesWhereMetricsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .whereFilter(predicate)
                .build();
        List<FilterPredicate.FilterParameter> params = predicate.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) AS highScore,"
                        + "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
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
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore) AS testScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "AND com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
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
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .whereFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore) AS testScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "WHERE (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "OR com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
                        + params.get(0).getPlaceholder()
                        + ") GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating";
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsOnly() throws Exception {
        FilterPredicate predicate = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .metric(invoke(playerStatsTable.getMetric("lowScore")))
                .havingFilter(predicate)
                .build();
        List<FilterPredicate.FilterParameter> params = predicate.getParameters();

//        String expectedQueryStr =
//                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore) AS testScore,"
//                        + "MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS lowScore "
//                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
//                        + "HAVING com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
//                        + params.get(0).getPlaceholder();
        String expectedQueryStr = "Manual Fail fix: FAILED: SemanticException HAVING specified without GROUP BY\n";
        System.out.println(expectedQueryStr);
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
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsAndDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(new AndFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

//        String expectedQueryStr =
//                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore) AS testScore,"
//                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
//                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
//                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating "
//                        + "HAVING (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
//                        + "AND com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
//                        + params.get(0).getPlaceholder() + ")";
        String expectedQueryStr = "Manual Fail: FAILED: SemanticException HAVING specified without GROUP BY";
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }

    @Test
    public void testShowQueriesHavingMetricsOrDims() throws Exception {
        FilterPredicate ratingFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "overallRating"),
                Operator.NOTNULL, new ArrayList<Object>());
        FilterPredicate highScoreFilter = new FilterPredicate(
                new Path(PlayerStats.class, dictionary, "testScore"),
                Operator.GT,
                Lists.newArrayList(9000));
        Query query = Query.builder()
                .table(playerStatsTable)
                .metric(invoke(playerStatsTable.getMetric("testScore")))
                .groupByDimension(toProjection(playerStatsTable.getDimension("overallRating")))
                .havingFilter(new OrFilterExpression(ratingFilter, highScoreFilter))
                .build();
        List<FilterPredicate.FilterParameter> params = highScoreFilter.getParameters();

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore) AS testScore,"
                        +"com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS overallRating "
                        + "FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats "
                        + "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating "
                        + "HAVING (com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating IS NOT NULL "
                        + "OR com_yahoo_elide_datastores_aggregation_example_PlayerStats.testScore > "
                        + params.get(0).getPlaceholder() + ")";
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryStr, engine.showQueries(query));
    }


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
        // Remove DISTINCT parens
        String expectedQueryStr1 =
                "SELECT COUNT(DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating, " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate) FROM " +
                        "playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats     ";
        String expectedQueryStr2 =
                "SELECT MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) AS " +
                        "lowScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
                        "overallRating,from_unixtime(unix_timestamp(" +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate, 'yyyy-MM-dd'), " +
                        "'yyyy-MM-dd') AS recordedDate FROM playerStats AS " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats   " +
                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating, " +
                        "from_unixtime(unix_timestamp(" +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStats.recordedDate, 'yyyy-MM-dd'), " +
                        "'yyyy-MM-dd')  ";
        List<String> expectedQueryList = new ArrayList<String>();
        expectedQueryList.add(expectedQueryStr1);
        expectedQueryList.add(expectedQueryStr2);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
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

//        String expectedQueryStr =
//                "SELECT DISTINCT com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
//                        "overallRating FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats      " +
//                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) DESC";
        String expectedQueryStr = "Manual Fail - FIX THIS";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        System.out.println(expectedQueryStr);
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

//        String expectedQueryStr =
//                //TODO This doesn't work in hive - Group by /order by / min
//                // https://stackoverflow.com/questions/23429710/hive-semanticexception-error-10002-line-321-invalid-column-reference-name
//                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStats.highScore) " +
//                        "AS highScore,com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating AS " +
//                        "overallRating FROM playerStats AS com_yahoo_elide_datastores_aggregation_example_PlayerStats " +
//                        "GROUP BY com_yahoo_elide_datastores_aggregation_example_PlayerStats.overallRating " +
//                        "ORDER BY MIN(com_yahoo_elide_datastores_aggregation_example_PlayerStats.lowScore) DESC";
//         "https://stackoverflow.com/questions/23429710/hive-semanticexception-error-10002-line-321-invalid-column-reference-name\n";

        String expectedQueryStr = "Manual fail fix this: Cannot Groupby and orderby different fields\n";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));

    }

    @Test
    public void testShowQuerySelectFromSubquery() {
        Query query = Query.builder()
                .table(playerStatsViewTable)
                .metric(invoke(playerStatsViewTable.getMetric("highScore")))
                .build();

        List<Object> results = StreamSupport.stream(engine.executeQuery(query, true).spliterator(), false)
                .collect(Collectors.toList());

        PlayerStatsView stats2 = new PlayerStatsView();
        stats2.setId("0");
        stats2.setHighScore(2412);

        String expectedQueryStr =
                "SELECT MAX(com_yahoo_elide_datastores_aggregation_example_PlayerStatsView.highScore) AS " +
                        "highScore FROM (SELECT stats.highScore, stats.player_id, c.name as countryName FROM " +
                        "playerStats AS stats LEFT JOIN countries AS c ON stats.country_id = c.id " +
                        "WHERE stats.overallRating = 'Great') AS " +
                        "com_yahoo_elide_datastores_aggregation_example_PlayerStatsView";
        List<String> expectedQueryList = Arrays.asList(expectedQueryStr);
        System.out.println(expectedQueryStr);
        compareQueryLists(expectedQueryList, engine.showQueries(query));
    }

}
