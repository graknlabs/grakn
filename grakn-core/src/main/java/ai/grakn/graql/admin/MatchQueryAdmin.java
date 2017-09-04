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

package ai.grakn.graql.admin;

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;

import javax.annotation.CheckReturnValue;
import java.util.Optional;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating a MatchQuery
 *
 * @author Felix Chapman
 */
public interface MatchQueryAdmin extends MatchQuery {

    /**
     * @param tx the {@link GraknTx} to use to get types from
     * @return all concept types referred to explicitly in the query
     */
    @CheckReturnValue
    Set<SchemaConcept> getSchemaConcepts(GraknTx tx);

    /**
     * @return all concept types referred to explicitly in the query
     */
    @CheckReturnValue
    Set<SchemaConcept> getSchemaConcepts();

    /**
     * @return the pattern to match in the graph
     */
    @CheckReturnValue
    Conjunction<PatternAdmin> getPattern();

    /**
     * @return the graph the query operates on, if one was provided
     */
    @CheckReturnValue
    Optional<GraknTx> tx();

    /**
     * @return all selected variable names in the query
     */
    @CheckReturnValue
    Set<Var> getSelectedNames();
}
