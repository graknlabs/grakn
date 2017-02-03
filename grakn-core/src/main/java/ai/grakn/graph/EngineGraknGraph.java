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

package ai.grakn.graph;


import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.EngineCache;
import ai.grakn.util.Schema;

import java.util.Set;

/**
 * <p>
 *     The Engine Specific Graph Interface
 * </p>
 *
 * <p>
 *     Provides common methods for advanced interaction with the graph.
 *     This is used internally by Engine
 * </p>
 *
 * @author fppt
 *
 */
public interface EngineGraknGraph extends BaseGraknGraph {

    /**
     * Commits the transaction without submitting any commit logs through the REST API.
     * COncepts in need of post processing are submitted directly to the {@link EngineCache}
     *
     * @throws GraknValidationException is thrown when a structural validation fails.
     */
    void commit(EngineCache cache) throws GraknValidationException;

    /**
     * Merges the provided duplicate castings.
     *
     * @param castingVertexIds The vertex Ids of the duplicate castings
     * @return if castings were merged and a commit is required.
     */
    boolean fixDuplicateCastings(Set<ConceptId> castingVertexIds);

    /**
     *
     * @param resourceVertexIds The resource vertex ids which need to be merged.
     * @return True if a commit is required.
     */
    boolean fixDuplicateResources(Set<ConceptId> resourceVertexIds);

    /**
     *
     * @param key The concept property tp search by.
     * @param value The value of the concept
     * @return A concept with the matching key and value
     */
    <T extends Concept> T  getConcept(Schema.ConceptProperty key, String value);
}
