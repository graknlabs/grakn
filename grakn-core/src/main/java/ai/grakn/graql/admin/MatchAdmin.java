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

package ai.grakn.graql.admin;

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;

import javax.annotation.CheckReturnValue;
import java.util.Set;

/**
 * Admin class for inspecting and manipulating a {@link Match}
 *
 * @author Grakn Warriors
 */
public interface MatchAdmin extends Match {

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
    GraknTx tx();

    /**
     * @return all selected variable names in the query
     */
    @CheckReturnValue
    Set<Var> getSelectedNames();

    /**
     * @return true if query will involve / set to performing inference
     */
    Boolean inferring();
}
