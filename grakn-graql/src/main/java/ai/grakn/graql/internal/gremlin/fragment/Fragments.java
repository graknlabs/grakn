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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;
import static ai.grakn.util.Schema.ConceptProperty.INSTANCE_TYPE_ID;
import static ai.grakn.util.Schema.EdgeLabel.SUB;

/**
 * Factory for creating instances of {@link Fragment}.
 *
 * @author Felix Chapman
 */
public class Fragments {

    private Fragments() {}

    public static Fragment inShortcut(
            VarName rolePlayer, VarName edge, VarName relation,
            Optional<TypeLabel> roleType, Optional<TypeLabel> relationType) {
        return new InShortcutFragment(rolePlayer, edge, relation, roleType, relationType);
    }

    public static Fragment outShortcut(
            VarName relation, VarName edge, VarName rolePlayer,
            Optional<TypeLabel> roleType, Optional<TypeLabel> relationType) {
        return new OutShortcutFragment(relation, edge, rolePlayer, roleType, relationType);
    }

    public static Fragment inSub(VarName start, VarName end) {
        return new InSubFragment(start, end);
    }

    public static Fragment outSub(VarName start, VarName end) {
        return new OutSubFragment(start, end);
    }

    public static InRelatesFragment inRelates(VarName start, VarName end) {
        return new InRelatesFragment(start, end);
    }

    public static Fragment outRelates(VarName start, VarName end) {
        return new OutRelatesFragment(start, end);
    }

    public static Fragment inIsa(VarName start, VarName end) {
        return new InIsaFragment(start, end, false);
    }

    public static Fragment outIsa(VarName start, VarName end) {
        return new OutIsaFragment(start, end, false);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static Fragment inIsaCastings(VarName start, VarName end) {
        return new InIsaFragment(start, end, true);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static Fragment outIsaCastings(VarName start, VarName end) {
        return new OutIsaFragment(start, end, true);
    }

    public static Fragment inHasScope(VarName start, VarName end) {
        return new InHasScopeFragment(start, end);
    }

    public static Fragment outHasScope(VarName start, VarName end) {
        return new OutHasScopeFragment(start, end);
    }

    public static Fragment dataType(VarName start, ResourceType.DataType dataType) {
        return new DataTypeFragment(start, dataType);
    }

    public static Fragment inPlays(VarName start, VarName end, boolean required) {
        return new InPlaysFragment(start, end, required);
    }

    public static Fragment outPlays(VarName start, VarName end, boolean required) {
        return new OutPlaysFragment(start, end, required);
    }

    public static Fragment inCasting(VarName start, VarName end) {
        return new InCastingFragment(start, end);
    }

    public static Fragment outCasting(VarName start, VarName end) {
        return new OutCastingFragment(start, end);
    }

    public static Fragment inRolePlayer(VarName start, VarName end) {
        return new InRolePlayerFragment(start, end);
    }

    public static Fragment outRolePlayer(VarName start, VarName end) {
        return new OutRolePlayerFragment(start, end);
    }

    public static Fragment id(VarName start, ConceptId id) {
        return new IdFragment(start, id);
    }

    public static Fragment label(VarName start, TypeLabel label) {
        return new LabelFragment(start, label);
    }

    public static Fragment value(VarName start, ValuePredicateAdmin predicate) {
        return new ValueFragment(start, predicate);
    }

    public static Fragment isAbstract(VarName start) {
        return new IsAbstractFragment(start);
    }

    public static Fragment regex(VarName start, String regex) {
        return new RegexFragment(start, regex);
    }

    public static Fragment notInternal(VarName start) {
        return new NotInternalFragment(start);
    }

    public static Fragment neq(VarName start, VarName other) {
        return new NeqFragment(start, other);
    }

    /**
     * A {@link Fragment} that uses an index stored on each resource. Resources are indexed by direct type and value.
     */
    public static Fragment resourceIndex(VarName start, TypeLabel typeLabel, Object resourceValue) {
        return new ResourceIndexFragment(start, typeLabel, resourceValue);
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> outSubs(GraphTraversal<Vertex, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `INSTANCE_TYPE_ID` property
        return traversal.union(__.not(__.has(INSTANCE_TYPE_ID.name())), __.repeat(__.out(SUB.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> inSubs(GraphTraversal<Vertex, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `INSTANCE_TYPE_ID` property
        return traversal.union(__.not(__.has(INSTANCE_TYPE_ID.name())), __.repeat(__.in(SUB.getLabel())).emit()).unfold();
    }

    static String displayOptionalTypeLabel(Optional<TypeLabel> typeLabel) {
        return typeLabel.map(label -> " " + typeLabelToString(label)).orElse("");
    }

    static void applyTypeLabelToTraversal(
            GraphTraversal<Vertex, Edge> traversal, Schema.EdgeProperty property, Optional<TypeLabel> typeLabel, GraknGraph graph) {
        typeLabel.ifPresent(label -> traversal.has(property.name(), graph.admin().convertToId(label)));
    }
}
