/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.test.migration.export;

import ai.grakn.test.kbs.MovieKB;
import ai.grakn.migration.export.KBWriter;
import ai.grakn.test.SampleKBContext;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.test.migration.export.KBWriterTestUtil.assertDataEqual;
import static ai.grakn.test.migration.export.KBWriterTestUtil.assertOntologiesEqual;
import static ai.grakn.test.migration.export.KBWriterTestUtil.insert;

public class MovieKBWriterTest {

    private KBWriter writer;

    @ClassRule
    public static SampleKBContext original = SampleKBContext.preLoad(MovieKB.get());

    @Rule
    public SampleKBContext copy = SampleKBContext.empty();

    @Before
    public void setup() {
        writer = new KBWriter(original.tx());
    }

    @Test
    public void testWritingMovieGraphSchema() {
        String schema = writer.dumpSchema();
        insert(copy.tx(), schema);

        assertOntologiesEqual(original.tx(), copy.tx());
    }

    @Test
    public void testWritingMovieGraphData() {
        String schema = writer.dumpSchema();
        insert(copy.tx(), schema);

        String data = writer.dumpData();
        insert(copy.tx(), data);

        assertDataEqual(original.tx(), copy.tx());
    }
}
