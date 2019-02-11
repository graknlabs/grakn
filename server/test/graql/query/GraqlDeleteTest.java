/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query;

import com.google.common.collect.Sets;
import grakn.core.graql.query.query.GraqlDelete;
import grakn.core.graql.query.query.MatchClause;
import grakn.core.graql.query.statement.Variable;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedHashSet;

import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class GraqlDeleteTest {

    private final MatchClause match1 = Graql.match(var("x").isa("movie"));
    private final MatchClause match2 = Graql.match(var("y").isa("movie"));

    private final Collection<Variable> vars1 = Sets.newHashSet(new Variable("x"));
    private final Collection<Variable> vars2 = Sets.newHashSet(new Variable("y"));

    @Test
    public void deleteQueriesWithTheSameMatchAndVarsAreEqual() {
        GraqlDelete query1 = new GraqlDelete(match1, new LinkedHashSet<>(vars1));
        GraqlDelete query2 = new GraqlDelete(match1, new LinkedHashSet<>(vars1));

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void deleteQueriesWithDifferentMatchesOrVarsAreDifferent() {
        GraqlDelete query1 = new GraqlDelete(match1, new LinkedHashSet<>(vars1));
        GraqlDelete query2 = new GraqlDelete(match2, new LinkedHashSet<>(vars2));

        assertNotEquals(query1, query2);
    }
}