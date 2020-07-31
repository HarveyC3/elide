/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.framework;

import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.filter.dialect.RSQLFilterDialect;
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
import com.yahoo.elide.datastores.aggregation.query.TimeDimensionProjection;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialectFactory;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialect;
import com.yahoo.elide.request.Argument;
import com.yahoo.elide.utils.ClassScanner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    }

    public static void init(){
        init(SQLDialectFactory.getDefaultDialect());
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

}
