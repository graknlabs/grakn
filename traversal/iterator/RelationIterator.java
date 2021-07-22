/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.traversal.iterator;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator.Sorted.Forwardable;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.RelationTraversal;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Optimised.ROLEPLAYER;

public class RelationIterator extends AbstractFunctionalIterator<VertexMap> {

    private final Collection<StructureVertex<?>> vertices;
    private final List<StructureEdge<?, ?>> edges;
    private final GraphTraversal.Parameters parameters;
    private final GraphManager graphMgr;

    private final Map<Integer, Forwardable<ThingVertex>> iterators;
    private final Map<Retrievable, Vertex<?, ?>> answer;
    private final Scoped scoped;
    private Retrievable relationId;
    private Set<Label> relationTypes;
    private ThingVertex relation;
    private State state;
    private int relationProposer;

    private enum State {INIT, EMPTY, PROPOSED, FETCHED, COMPLETED}

    public RelationIterator(Traversal traversal, GraphManager graphMgr) {
        this.graphMgr = graphMgr;
        this.parameters = traversal.parameters();
        vertices = traversal.structure().vertices();
        edges = new ArrayList<>(traversal.structure().edges());
        answer = new HashMap<>();
        iterators = new HashMap<>();
        scoped = new Scoped();
        state = State.INIT;
        relationProposer = 0;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                computeFirst();
                return state == State.FETCHED;
            case EMPTY:
                computeNext();
                return state == State.FETCHED;
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            case PROPOSED:
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    @Override
    public VertexMap next() {
        if (!hasNext()) throw new NoSuchElementException();
        VertexMap vertexMap = VertexMap.of(answer);
        state = State.EMPTY;
        return vertexMap;
    }

    private void computeFirst() {
        assert state == State.INIT;
        if (tryInitialise() && proposeFirst()) {
            state = State.PROPOSED;
            while (state == State.PROPOSED) {
                verifyProposed();
            }
        } else {
            state = State.COMPLETED;
        }
    }

    private boolean tryInitialise() {
        StructureVertex.Thing relationVertex = relationVertex();
        relationId = relationVertex.id().asVariable().asRetrievable();
        relationTypes = relationVertex.asThing().props().types();
        return tryInitialiseFixedPlayers();
    }

    private StructureVertex.Thing relationVertex() {
        List<StructureVertex<?>> withoutIID = iterate(vertices)
                .filter(vertex -> !parameters.getIdentifiersWithIID().contains(vertex.id().asVariable().asRetrievable())).toList();
        assert withoutIID.size() == 1;
        return withoutIID.get(0).asThing();
    }

    private boolean tryInitialiseFixedPlayers() {
        for (Identifier.Variable withIID : parameters.getIdentifiersWithIID()) {
            assert withIID.isRetrievable();
            ThingVertex thingVertex = graphMgr.data().getReadable(parameters.getIID(withIID));
            if (thingVertex == null) {
                state = State.COMPLETED;
                return false;
            }
            answer.put(withIID.asRetrievable(), thingVertex);
        }
        return true;
    }

    private boolean proposeFirst() {
        assert state == State.INIT && relation == null && relationProposer == 0;
        FunctionalIterator.Sorted<ThingVertex> relationIterator = getIterator(relationProposer);
        if (relationIterator.hasNext()) {
            relation = relationIterator.next();
            return true;
        } else {
            return false;
        }
    }

    private void computeNext() {
        if (proposeNext()) {
            state = State.PROPOSED;
            while (state == State.PROPOSED) {
                verifyProposed();
            }
        } else {
            state = State.COMPLETED;
        }
    }

    private boolean proposeNext() {
        assert state == State.EMPTY;
        FunctionalIterator.Sorted<ThingVertex> relationIterator = getIterator(relationProposer);
        scoped.clear();
        while (relationIterator.hasNext()) {
            ThingVertex newRelation = relationIterator.next();
            if (!newRelation.equals(relation)) {
                relation = newRelation;
                return true;
            }
            scoped.clear();
        }
        return false;
    }

    private void verifyProposed() {
        for (int i = 0; i < edges.size(); i++) {
            if (i != relationProposer && !verifyProposed(i)) return;
        }
        answer.put(relationId, relation);
        state = State.FETCHED;
    }

    private boolean verifyProposed(int edge) {
        Forwardable<ThingVertex> relationIterator = getIterator(edge);
        if (!relationIterator.hasNext()) {
            state = State.COMPLETED;
            return false;
        }
        ThingVertex relation = relationIterator.peek();
        int comparison = relation.compareTo(this.relation);
        if (comparison == 0) {
            return true;
        } else if (comparison < 0) {
            relationIterator.forward(this.relation);
            return verifyProposed(edge);
        } else {
            propose(edge, relationIterator.next());
            return false;
        }
    }

    private void propose(int proposer, ThingVertex proposedRelation) {
        relationProposer = proposer;
        relation = proposedRelation;
        scoped.clearExcept(proposer);
        state = State.PROPOSED;
    }

    private Forwardable<ThingVertex> getIterator(int edge) {
        assert edges.get(edge).to().id().isRetrievable();
        return iterators.computeIfAbsent(edge, this::createIterator);
    }

    private Forwardable<ThingVertex> createIterator(int edge) {
        StructureEdge<?, ?> structureEdge = edges.get(edge);
        Retrievable playerId = structureEdge.to().id().asVariable().asRetrievable();
        ThingVertex player = answer.get(playerId).asThing();
        assert relationVertex().asThing().props().types().size() == 1; // TODO relax this
        TypeVertex relType = graphMgr.schema().getType(relationVertex().asThing().props().types().iterator().next());
        List<Forwardable<KeyValue<ThingVertex, ThingVertex>>> relationIterators = new ArrayList<>();
        for (Label roleLabel : structureEdge.asNative().asRolePlayer().types()) {
            TypeVertex roleType =  graphMgr.schema().getType(roleLabel);
            relationIterators.add(player.ins()
                    .edge(ROLEPLAYER, roleType, PrefixIID.of(relType.iid().encoding().instance()), relType.iid()).get()
                    .mapSorted(
                            directedEdge -> {
                                ThingVertex role = directedEdge.getEdge().optimised().get();
                                ThingVertex relation = directedEdge.getEdge().from();
                                return new KeyValue<>(relation, role);
                            }, relationRole -> {
                                ThingEdge target = new ThingEdgeImpl.Target(ROLEPLAYER, relationRole.key(), player, roleType);
                                return ThingAdjacency.DirectedEdge.in(target);
                            }));
        }
        Forwardable<KeyValue<ThingVertex, ThingVertex>> relationRoles = Iterators.Sorted.merge(relationIterators);
        return relationRoles.filter(relationRole -> !scoped.containsRole(relationRole.value()))
                .mapSorted(relationRole -> {
                    scoped.record(edge, relationRole.value());
                    return relationRole.key();
                }, relation -> new KeyValue<>(relation, null));
    }

    @Override
    public void recycle() {
        iterators.values().forEach(FunctionalIterator::recycle);
    }

    private static class Scoped {

        private final Map<Integer, ThingVertex> scoped;
        private final Set<ThingVertex> scopedSet;

        Scoped() {
            scoped = new HashMap<>();
            scopedSet = new HashSet<>();
        }

        public void record(Integer edge, ThingVertex role) {
            ThingVertex previousScoped = scoped.put(edge, role);
            scopedSet.remove(previousScoped);
            scopedSet.add(role);
        }

        public void clearExcept(Integer edge) {
            Iterator<Map.Entry<Integer, ThingVertex>> iterator = scoped.entrySet().iterator();
            iterator.forEachRemaining(entry -> {
                if (!entry.getKey().equals(edge)) {
                    iterator.remove();
                    scopedSet.remove(entry.getValue());
                }
            });
        }

        public boolean containsRole(ThingVertex optimised) {
            return scopedSet.contains(optimised);
        }

        public void clear() {
            scoped.clear();
            scopedSet.clear();
        }
    }
}
