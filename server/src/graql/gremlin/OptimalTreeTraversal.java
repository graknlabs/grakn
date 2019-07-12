package grakn.core.graql.gremlin;

import com.google.common.collect.Sets;
import grakn.core.api.Transaction;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.spanningtree.Arborescence;
import grakn.core.graql.gremlin.spanningtree.graph.Node;
import grakn.core.graql.gremlin.spanningtree.graph.NodeId;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.server.session.TransactionOLTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static grakn.core.graql.gremlin.NodesUtil.propagateLabels;

public class OptimalTreeTraversal {

    private TransactionOLTP tx;
    int iterations;
    int productIterations;
//    int shortCircuits;

    public OptimalTreeTraversal(TransactionOLTP tx) {

        this.tx = tx;
        iterations = 0;
        productIterations = 0;
//        shortCircuits = 0;
    }

    public Map<Set<Node>, Pair<Double, Set<Node>>> traverse(Arborescence<Node> arborescence,
                                                            Map<NodeId, Node> nodes,
                                                            Map<Node, Map<Node, Fragment>> edgeFragmentChildToParent) {

        List<Fragment> plan = new ArrayList<>();

        Node root = arborescence.getRoot();

        Map<Node, Set<Node>> edgesParentToChild = new HashMap<>();
        arborescence.getParents().forEach((child, parent) -> {
            if (!edgesParentToChild.containsKey(parent)) {
                edgesParentToChild.put(parent, new HashSet<>());
            }
            edgesParentToChild.get(parent).add(child);
        });

        propagateLabels(edgesParentToChild);

        Set<Node> visited = Sets.newHashSet(root);
        Set<Node> reachable = edgesParentToChild.get(root);
        Map<Set<Node>, Pair<Double, Set<Node>>> memoisedResults = new HashMap<>();

        optimal_cost(visited, reachable, memoisedResults, edgesParentToChild);

        return memoisedResults;
    }

    public double optimal_cost(Set<Node> visited,
                               Set<Node> reachable,
                               Map<Set<Node>, Pair<Double, Set<Node>>> memoised,
                               Map<Node, Set<Node>> edgesParentToChild) {
        iterations++;
        // if we have visited this exact set before, we can return
        if (memoised.containsKey(visited)) {
            return memoised.get(visited).getKey();
        }

        double cost = product(visited);
        if (reachable.size() == 0) {
            memoised.put(visited, new Pair<>(cost, null));
            return cost;
        }

        double bestCost = 0;
        Set<Node> bestNextVisited = null;
        for (Node node : reachable) {
            // copy
            Set<Node> nextVisited = new HashSet<>(visited);
            nextVisited.add(node);

            // copy
            Set<Node> nextReachable = new HashSet<>(reachable);
            if (edgesParentToChild.containsKey(node)) {
                nextReachable.addAll(edgesParentToChild.get(node));
            }
            nextReachable.remove(node);

            double nextCost = cost + optimal_cost(nextVisited, nextReachable, memoised, edgesParentToChild);

            if (bestNextVisited == null || nextCost < bestCost) {
                bestCost = nextCost;
                bestNextVisited = nextVisited;
            }
        }

        // update memoised result
        memoised.put(visited, new Pair<>(bestCost, bestNextVisited));

        return bestCost;
    }

    class StackEntry {
        boolean haveVisited;
        Set<Node> visited;
        Set<Node> reachable;
        List<Set<Node>> children;

        public StackEntry(Set<Node> visited, Set<Node> reachable) {
            this.visited = visited;
            this.reachable = reachable;
            haveVisited = false;
            children = new ArrayList<>();
        }

        public void addChild(Set<Node> child) {
            this.children.add(child);
        }

        public void setHaveVisited() {
            haveVisited = true;
        }
    }

    public double optimal_cost_stack(Node root,
//                                     Map<Set<Node>, Pair<Double, Set<Node>>> memoised,
                                     Map<Integer, Pair<Double, Set<Node>>> memoised,
                                     Map<Node, Set<Node>> edgesParentToChild) {

        Set<Node> visited = Sets.newHashSet(root);
        Set<Node> reachable = edgesParentToChild.get(root);

        Stack<StackEntry> stack = new Stack<>();
        stack.push(new StackEntry(visited, reachable));

        long timeSpentInMemoised = 0l;
        long timeSpentInElse = 0l;
        long timeSpentInProduct = 0l;
        long start1;
        long start2;
        long start3;

//        double currentCost = 0.0;
//        double bestFoundCost = Double.MAX_VALUE;

        while (stack.size() != 0) {
            iterations++;
            StackEntry entry = stack.peek();

            // compute the cost of the current visited set
            start3 = System.nanoTime();
            double cost = product(entry.visited);
            timeSpentInProduct += (System.nanoTime() - start3);

            if (entry.haveVisited) {
                // actually remove this stack entry when we see it the second time
                stack.pop();

                // find the best child to choose next
                Set<Node> bestChild = null;
                double bestChildCost = 0.0;
                if (entry.children.size() > 0) {
                    // update the cost to include the path from the best to child to the finish
                    // if there are any children
                    for (Set<Node> child : entry.children) {
                        start1 = System.nanoTime();
                        Pair<Double, Set<Node>> memoisedResult = memoised.get(child.hashCode());
                        timeSpentInMemoised += (System.nanoTime() - start1);
                        if (memoisedResult != null && (bestChild == null || memoisedResult.getKey() < bestChildCost)) {
                            bestChild = child;
                            bestChildCost = memoisedResult.getKey();
                        }
                    }
                }

                // memoise the cost
                start1 = System.nanoTime();
                memoised.put(entry.visited.hashCode(), new Pair<>(cost + bestChildCost, bestChild));
                timeSpentInMemoised += (System.nanoTime() - start1);

//                // update the global best cost found yet if we are the top of the stack and have no more paths to explore
//                if (entry.children.size() == 0 && currentCost < bestFoundCost) {
//                    bestFoundCost = currentCost;
//                }

//                 remove the cost from the current total as we have finished processing this stack entry
//                currentCost -= cost;

            } else {
                start2 = System.nanoTime();
                // set that we have visited the entry for the first time

//                // this branch means we are expanding this path of exploration
//                // so the current cost includes this branch
//                currentCost += cost;

                entry.setHaveVisited();

//                // This never short circuits because the last term of the cost always dominates
//                if (currentCost > bestFoundCost) {
//                    // we can short circuit the execution if we are on a more expensive branch than the best yet
//                    shortCircuits++;
//                    stack.pop();
//                    currentCost -= cost;
//                    continue;
//                }

                for (Node nextNode : entry.reachable) {
                    Set<Node> nextVisited = new HashSet<>(entry.visited);
                    nextVisited.add(nextNode);

                    // record that this child is a dependant of the currently stack entry
                    entry.addChild(nextVisited);

                    // compute child if we don't already have the result for the child
                    start1 = System.nanoTime();
                    boolean isComputed = memoised.containsKey(nextVisited.hashCode());
                    timeSpentInMemoised += (System.nanoTime() - start1);
                    if (!isComputed) {
                        Set<Node> nextReachable = new HashSet<>(entry.reachable);
                        if (edgesParentToChild.get(nextNode) != null) {
                            nextReachable.addAll(edgesParentToChild.get(nextNode));
                        }
                        nextReachable.remove(nextNode);
                        stack.add(new StackEntry(nextVisited, nextReachable));
                    }
                }
                timeSpentInElse += (System.nanoTime() - start2);
            }
        }


        System.out.println("Time spent in memoised: " + (timeSpentInMemoised/1000000.0));
        System.out.println("Time spent in else: " + (timeSpentInElse/1000000.0));
        System.out.println("Time spent in product: " + (timeSpentInProduct/1000000.0));
        return memoised.get(Sets.newHashSet(root).hashCode()).getKey();
    }


    private double product(Set<Node> nodes) {
        productIterations++;
        double cost = 1.0;
        // this is an expensive operation - it may be the mocks in tests that are slow, as writing constants for node.matchingElementsEstimate() is much faster
        for (Node node : nodes) {
            cost = cost * node.matchingElementsEstimate(tx);
        }
        return cost;
    }

}

