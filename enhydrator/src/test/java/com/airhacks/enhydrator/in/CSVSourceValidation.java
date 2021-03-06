/*
 * Copyright 2014 Adam Bien.
 *
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
 */
package com.airhacks.enhydrator.in;

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
import java.util.Iterator;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public abstract class CSVSourceValidation {

    protected Source cut;

    @Test
    public void query() {
        Iterable<Row> cars = this.cut.query();
        int counter = 0;
        for (Row row : cars) {
            counter++;
            assertThat(row.getNumberOfColumns(), is(4));
            System.out.println("List: " + row);
        }
        //Header is included
        assertThat(counter, is(4));
    }

    @Test
    public void columnNameIsSet() {
        Iterable<Row> cars = this.cut.query();
        int counter = 0;
        boolean headerSkipped = false;
        for (Row list : cars) {
            Column entry = list.getColumnByName("Year");
            if (!headerSkipped) {
                assertThat(entry.getValue(), is("Year"));
                headerSkipped = true;
                continue;
            }
            Integer.parseInt(entry.getValue().toString());
            //Year;Make;Model;Length
            assertNotNull(entry);
            counter++;

        }
        //Header not included
        assertThat(counter, is(3));
    }

    @Test
    public void columnIndexIsSet() {
        Source source = getSource("./src/test/files/index.csv");
        Iterable<Row> pyramid = source.query();

        Iterator<Row> iterator = pyramid.iterator();
        iterator.next(); //skip header
        Row contentRow = iterator.next();
        for (int i = 0; i < 5; i++) {
            Column entry = contentRow.getColumnByIndex(i);
            assertNotNull(entry);
            assertThat(entry.getName(), is(String.valueOf(4 - i)));
            assertThat(entry.getValue(), is(String.valueOf(i)));
        }
    }

    @Test
    public void nullHandling() {
        Source vss = getSource("./src/test/files/nullcolumns.csv");
        Iterable<Row> query = vss.query();
        Iterator<Row> iterator = query.iterator();
        iterator.next(); //skipping header
        Row first = iterator.next();
        assertNotNull(first);
        assertNull(first.getColumnValue("1"));
        assertNull(first.getColumnValue("2"));
        assertNull(first.getColumnValue("3"));
        assertNull(first.getColumnValue("4"));

        Row second = iterator.next();
        String emptyString = " ";
        assertThat(second.getColumnValue("1"), is(emptyString));
        assertThat(second.getColumnValue("2"), is(emptyString));
        assertThat(second.getColumnValue("3"), is(emptyString));
        assertNull(second.getColumnValue("4"));
    }

    @Test
    public void splitEmtpyRows() {
        String[] split = CSVFileSource.split(";;", ";");
        assertNotNull(split);
        assertThat(split.length, is(3));
        for (String column : split) {
            assertTrue(column.isEmpty());
        }
    }

    @Test
    public void splitEmptyStrings() {
        String[] split = CSVFileSource.split(" ; ; ", ";");
        assertNotNull(split);
        assertThat(split.length, is(3));
        for (String column : split) {
            assertThat(column, is(" "));
        }
    }

    @Test
    public void pyramid() {
        Source vss = getSource("./src/test/files/pyramid.csv");
        Iterable<Row> query = vss.query();
        int counter = 1;
        boolean readHeader = false;
        String columnName;
        for (Row list : query) {
            if (!readHeader) {
                readHeader = true;
                continue;
            }
            assertThat(list.getNumberOfColumns(), is(counter++));
            columnName = String.valueOf(counter - 1);
            assertThat(list.getColumnValue(columnName), is(columnName));
            assertNull(list.getColumnValue(String.valueOf(42)));
        }
    }

    public abstract Source getSource(final String fileName);

}
