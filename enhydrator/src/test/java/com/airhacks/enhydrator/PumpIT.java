package com.airhacks.enhydrator;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.airhacks.enhydrator.flexpipe.Pipeline;
import com.airhacks.enhydrator.flexpipe.PipelineTest;
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author airhacks.com
 */
public class PumpIT {

    JDBCSource source;

    @Before
    public void initialize() {
        this.source = new JDBCSource.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
    }

    @Test
    public void oneToOneTransformationWithName() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                with("name", t -> t.asList()).
                to(consumer).sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    static Sink getMockedSink() {
        Sink mock = mock(Sink.class);
        when(mock.getName()).thenReturn("*");
        return mock;
    }

    @Test
    public void oneToOneTransformationWithIndex() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                with(1, t -> t.changeValue("duke").asList()).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void ignoringPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        final ArrayList<Entry> entries = new ArrayList<>();
        Pump pump = new Pump.Engine().
                from(source).
                startWith(l -> null).
                sqlQuery("select * from Coffee").
                to(consumer).
                build();
        pump.start();
        verify(consumer, never()).processRow(entries);
    }

    @Test
    public void postPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                endWith(l -> l).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void passThrough() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void ignoringFilter() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                filter("false").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").build();
        long rowCount = pump.start();
        //counts all rows, not processed rows
        assertThat(rowCount, is(2l));
        verify(consumer, never()).processRow(any(List.class));
    }

    @Test
    public void acceptingFilter() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                filter("true").
                filter("columns.empty === false").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        long rowCount = pump.start();
        assertThat(rowCount, is(2l));
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void scriptEntryTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                from(source).
                with(1, "quote").
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));

    }

    @Test
    public void scriptRowTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                startWith("reverse").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));

    }

    @Test
    public void usePipeline() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Pipeline pipeline = PipelineTest.getPipeline();
        Sink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                use(pipeline).
                to(consumer).
                build();
        pump.start();
        verify(consumer).processRow(any(List.class));
    }

    @Test
    public void usePipelineWithSink() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Pipeline pipeline = PipelineTest.getPipeline();
        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                use(pipeline).
                build();
        pump.start();
    }

    @Test
    public void applyExpressionsWithoutExpressions() {
        Pump pump = new Pump.Engine().
                build();
        List<Entry> entries = getEntries();
        final Entry expected = entries.get(0);
        List<Entry> result = pump.applyExpressions(entries, expected);
        assertThat(result, CoreMatchers.hasItem(expected));
        assertThat(result.size(), is(1));
    }

    @Test
    public void applyRowTransformationsWithoutFunctions() {
        List<Entry> input = getEntries();
        List<Entry> output = Pump.applyRowTransformations(new ArrayList<>(), input);
        assertThat(output, is(input));
    }

    @Test
    public void applyRowTransformationsWitDevNull() {
        List<Entry> input = getEntries();
        List<Function<List<Entry>, List<Entry>>> funcs = new ArrayList<>();
        funcs.add(l -> new ArrayList<>());
        List<Entry> output = Pump.applyRowTransformations(funcs, input);
        assertTrue(output.isEmpty());
    }

    @Test
    public void groupByDefaultDestination() {
        List<Entry> entries = getEntries();
        Pump pump = new Pump.Engine().
                build();
        Map<String, List<Entry>> grouped = pump.groupByDestinations(entries);
        assertThat(grouped.size(), is(1));
        List<Entry> unclassified = grouped.get("*");
        assertThat(unclassified, is(entries));
    }

    @Test
    public void groupByCustomDestinations() {
        List<Entry> entries = getEntries();
        final String destination = "cuttingEdge";
        entries.add(new Entry(3, "something", "java").changeDestination(destination));
        entries.add(new Entry(4, "something", "jvm").changeDestination(destination));
        Pump pump = new Pump.Engine().
                build();
        Map<String, List<Entry>> grouped = pump.groupByDestinations(entries);
        assertThat(grouped.size(), is(2));
        List<Entry> unclassified = grouped.get("*");
        unclassified.forEach(entry -> assertThat(entry.getDestination(), is("*")));
        List<Entry> byDestination = grouped.get(destination);
        byDestination.forEach(entry -> assertThat(entry.getDestination(), is(destination)));
    }

    @Test
    public void groupByCustomDestinationsWithMisconfiguredSink() {
        List<Entry> entries = getEntries();
        final String destination = "cuttingEdge";
        entries.add(new Entry(3, "something", "java").changeDestination(destination));
        entries.add(new Entry(4, "something", "jvm").changeDestination(destination));
        Sink sink = getMockedSink();
        Pump pump = new Pump.Engine().to(sink).
                build();
        Map<String, List<Entry>> grouped = pump.groupByDestinations(entries);
        assertThat(grouped.size(), is(2));
        List<Entry> unclassified = grouped.get("*");
        unclassified.forEach(entry -> assertThat(entry.getDestination(), is("*")));
        List<Entry> byDestination = grouped.get(destination);
        byDestination.forEach(entry -> assertThat(entry.getDestination(), is(destination)));
    }

    List<Entry> getEntries() {
        List<Entry> row = new ArrayList<>();
        row.add(new Entry(0, "a", 42, "java"));
        row.add(new Entry(1, "b", 21, "tengah"));
        return row;
    }

    @After
    public void clearTables() {
        CoffeeTestFixture.deleteTables();
    }

}