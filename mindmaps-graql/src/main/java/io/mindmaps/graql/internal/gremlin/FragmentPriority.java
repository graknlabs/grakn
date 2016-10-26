/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

/**
 * represents the priority of a {@code Fragment}.
 * <p>
 * Higher priority patterns are expected to be more efficient and better filter the query and are executed first.
 */
public enum FragmentPriority {

    /**
     * Looking up things by value. Values are non-unique so this is only slightly slower than lookup by ID
     */
    VALUE_SPECIFIC,

    /**
     * Looking up by ID.
     * Because types are often super-nodes and are usually referred to by ID, it is usually better to start from an
     * instance (looked up by value) than from a type (looked up by ID) if possible.
     */
    ID,

    /**
     * Looking up things by non-specific values. This includes predicates (e.g. 'value > 100') and very short strings,
     * which are assumed to be common.
     */
    VALUE_NONSPECIFIC,

    /**
     * Moving along an edge, where a concept can be expected to have at most one (e.g. ISA, SUB)
     */
    EDGE_UNIQUE,

    /**
     * Moving along an edge, where a concept can be expected to have a small number bounded by the ontology
     * (e.g. PLAYS_ROLE, HAS_ROLE)
     */
    EDGE_BOUNDED,

    /**
     * Moving along a relation edge, where a concept can be expected to have a large number of relations.
     */
    EDGE_RELATION,

    /**
     * Moving along an edge that a concept may have a HUGE number of, e.g. all instances of a type
     */
    EDGE_UNBOUNDED,

    /**
     * Confirming that castings are all distinct
     */
    // TODO: Should this have higher priority?
    DISTINCT_CASTING;

}
