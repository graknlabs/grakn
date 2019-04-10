package grakn.core.graql.gremlin;

import com.google.common.collect.Sets;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.core.graql.gremlin.NodesUtil.nodeToPlanFragments;

public class GreedyTreeToPlan {

    // standard tree traversal from the root node
    // always visit the branch/node with smaller cost
    static List<Fragment> greedyTraversal(Arborescence<Node> arborescence,
                                                  Map<NodeId, Node> nodes,
                                                  Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        List<Fragment> plan = new LinkedList<>();

        Map<Node, Set<Node>> edgesParentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!edgesParentToChild.containsKey(parent)) {
                edgesParentToChild.put(parent, new HashSet<>());
            }
            edgesParentToChild.get(parent).add(child);
        });

        Node root = arborescence.getRoot();

        Set<Node> reachableNodes = Sets.newHashSet(root);
        // expanding from the root until all nodes have been visited
        while (!reachableNodes.isEmpty()) {

            Node nodeWithMinCost = reachableNodes.stream().min(Comparator.comparingDouble(node ->
                    branchWeight(node, arborescence, edgesParentToChild, edgeFragmentChildToParent))).orElse(null);

            assert nodeWithMinCost != null : "reachableNodes is never empty, so there is always a minimum";

            // add edge fragment first, then node fragments
            Fragment fragment = getEdgeFragment(nodeWithMinCost, arborescence, edgeFragmentChildToParent);
            if (fragment != null) plan.add(fragment);

            plan.addAll(nodeToPlanFragments(nodeWithMinCost, nodes, true));

            reachableNodes.remove(nodeWithMinCost);
            if (edgesParentToChild.containsKey(nodeWithMinCost)) {
                reachableNodes.addAll(edgesParentToChild.get(nodeWithMinCost));
            }
        }

        return plan;
    }

    // recursively compute the weight of a branch
    private static double branchWeight(Node node, Arborescence<Node> arborescence,
                                       Map<Node, Set<Node>> edgesParentToChild,
                                       Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        Double nodeWeight = node.getNodeWeight();

        if (nodeWeight == null) {
            nodeWeight = getEdgeFragmentCost(node, arborescence, edgeFragmentChildToParent) + nodeFragmentWeight(node);
            node.setNodeWeight(nodeWeight);
        }

        Double branchWeight = node.getBranchWeight();

        if (branchWeight == null) {
            final double[] weight = {nodeWeight};
            if (edgesParentToChild.containsKey(node)) {
                edgesParentToChild.get(node).forEach(child ->
                        weight[0] += branchWeight(child, arborescence, edgesParentToChild, edgeFragmentChildToParent));
            }
            branchWeight = weight[0];
            node.setBranchWeight(branchWeight);
        }

        return branchWeight;
    }

    // compute the total cost of a node
    private static double nodeFragmentWeight(Node node) {
        double costFragmentsWithoutDependency = node.getFragmentsWithoutDependency().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        double costFragmentsWithDependencyVisited = node.getFragmentsWithDependencyVisited().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        double costFragmentsWithDependency = node.getFragmentsWithDependency().stream()
                .mapToDouble(Fragment::fragmentCost).sum();
        return costFragmentsWithoutDependency + node.getFixedFragmentCost() +
                (costFragmentsWithDependencyVisited + costFragmentsWithDependency) / 2D;
    }

    // get edge fragment cost in order to map branch cost
    private static double getEdgeFragmentCost(Node node, Arborescence<Node> arborescence,
                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Fragment fragment = getEdgeFragment(node, arborescence, edgeToFragment);
        if (fragment != null) return fragment.fragmentCost();

        return 0D;
    }

    @Nullable
    private static Fragment getEdgeFragment(Node node, Arborescence<Node> arborescence,
                                            Map<Node, Map<Node, Fragment>> edgeToFragment) {
        if (edgeToFragment.containsKey(node) &&
                edgeToFragment.get(node).containsKey(arborescence.getParents().get(node))) {
            return edgeToFragment.get(node).get(arborescence.getParents().get(node));
        }
        return null;
    }


}
