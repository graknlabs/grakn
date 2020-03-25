/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2020 Grakn Labs Ltd
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


package grakn.core.graql.reasoner.tree;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ResolutionTree {

    private final Node rootNode;
    private final Map<ResolutionState, Node> mapping = new HashMap<>();

    public ResolutionTree(ResolutionState rootState){
        this.rootNode = new NodeImpl(rootState);
        mapping.put(rootState, rootNode);
    }

    public Node getNode(ResolutionState state){
        return mapping.get(state);
    }

    public Set<Node> getNodes(){
        return new HashSet<>(mapping.values());
    }

    public void clear(){
        mapping.clear();
    }

    public void updateTree(ResolutionState state) {
        ResolutionState parent = state.getParentState();
        if (parent == null) return;

        if (state.isAnswerState()) {
            Node parentNode = getNode(parent);
            if (parentNode != null){
                ConceptMap sub = state.getSubstitution();
                parentNode.addAnswer(sub);
            }
        } else {
            addChildToNode(parent, state);
        }
    }

    Node addChildToNode(ResolutionState parent, ResolutionState child){
        Node parentMatch = mapping.get(parent);
        Node childMatch = mapping.get(child);
        Node parentNode = parentMatch != null? parentMatch : new NodeImpl(parent);
        Node childNode = childMatch != null? childMatch : new NodeImpl(child);

        parentNode.addChild(childNode);
        if (parentMatch == null) mapping.put(parent, parentNode);
        if (childMatch == null ) mapping.put(child, childNode);
        return childNode;
    }
}

