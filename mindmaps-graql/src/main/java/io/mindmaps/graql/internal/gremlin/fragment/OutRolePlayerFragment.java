package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.EdgeLabel.ROLE_PLAYER;

class OutRolePlayerFragment extends AbstractFragment {

    OutRolePlayerFragment(String start, String end) {
        super(start, end);
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        traversal.out(ROLE_PLAYER.getLabel());
    }

    @Override
    public String getName() {
        return "-[role-player]->";
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.EDGE_UNIQUE;
    }
}
