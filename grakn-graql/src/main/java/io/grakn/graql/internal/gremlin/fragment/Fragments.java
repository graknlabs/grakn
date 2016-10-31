package io.grakn.graql.internal.gremlin.fragment;

import io.grakn.concept.ResourceType;
import io.grakn.graql.admin.ValuePredicateAdmin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static io.grakn.util.Schema.EdgeLabel.SUB;

public class Fragments {

    private Fragments() {}

    public static Fragment shortcut(
            Optional<String> relationType, Optional<String> roleStart, Optional<String> roleEnd,
            String start, String end
    ) {
        return new ShortcutFragment(relationType, roleStart, roleEnd, start, end);
    }

    public static Fragment inSub(String start, String end) {
        return new InSubFragment(start, end);
    }

    public static Fragment outSub(String start, String end) {
        return new OutSubFragment(start, end);
    }

    public static InHasRoleFragment inHasRole(String start, String end) {
        return new InHasRoleFragment(start, end);
    }

    public static Fragment outHasRole(String start, String end) {
        return new OutHasRoleFragment(start, end);
    }

    public static InIsaFragment inIsa(String start, String end) {
        return new InIsaFragment(start, end, false);
    }

    public static OutIsaFragment outIsa(String start, String end) {
        return new OutIsaFragment(start, end, false);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static InIsaFragment inIsaCastings(String start, String end) {
        return new InIsaFragment(start, end, true);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static OutIsaFragment outIsaCastings(String start, String end) {
        return new OutIsaFragment(start, end, true);
    }

    public static InHasScopeFragment inHasScope(String start, String end) {
        return new InHasScopeFragment(start, end);
    }

    public static OutHasScopeFragment outHasScope(String start, String end) {
        return new OutHasScopeFragment(start, end);
    }

    public static DataTypeFragment dataType(String start, ResourceType.DataType dataType) {
        return new DataTypeFragment(start, dataType);
    }

    public static InPlaysRoleFragment inPlaysRole(String start, String end) {
        return new InPlaysRoleFragment(start, end);
    }

    public static OutPlaysRoleFragment outPlaysRole(String start, String end) {
        return new OutPlaysRoleFragment(start, end);
    }

    public static InCastingFragment inCasting(String start, String end) {
        return new InCastingFragment(start, end);
    }

    public static OutCastingFragment outCasting(String start, String end) {
        return new OutCastingFragment(start, end);
    }

    public static InRolePlayerFragment inRolePlayer(String start, String end) {
        return new InRolePlayerFragment(start, end);
    }

    public static OutRolePlayerFragment outRolePlayer(String start, String end) {
        return new OutRolePlayerFragment(start, end);
    }

    public static DistinctCastingFragment distinctCasting(String start, String otherCastingName) {
        return new DistinctCastingFragment(start, otherCastingName);
    }

    public static IdFragment id(String start, String id) {
        return new IdFragment(start, id);
    }

    public static ValueFragment value(String start, ValuePredicateAdmin predicate) {
        return new ValueFragment(start, predicate);
    }

    public static RhsFragment rhs(String start, String rhs) {
        return new RhsFragment(start, rhs);
    }

    public static LhsFragment lhs(String start, String lhs) {
        return new LhsFragment(start, lhs);
    }

    public static IsAbstractFragment isAbstract(String start) {
        return new IsAbstractFragment(start);
    }

    public static RegexFragment regex(String start, String regex) {
        return new RegexFragment(start, regex);
    }

    public static ValueFlagFragment value(String start) {
        return new ValueFlagFragment(start);
    }

    public static NotCastingFragment notCasting(String start) {
        return new NotCastingFragment(start);
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> outSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.out(SUB.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> inSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.in(SUB.getLabel())).emit()).unfold();
    }
}
