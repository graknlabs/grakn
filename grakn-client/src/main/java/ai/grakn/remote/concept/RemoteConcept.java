/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptMethod;
import ai.grakn.remote.RemoteGraknTx;

import java.util.Objects;

/**
 * @author Felix Chapman
 */
abstract class RemoteConcept<Self extends Concept> implements Concept {

    abstract RemoteGraknTx tx();

    @Override
    public abstract ConceptId getId();

    @Override
    public final Keyspace keyspace() {
        return tx().keyspace();
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        runVoidMethod(ConceptMethod.DELETE);
    }

    @Override
    public final boolean isDeleted() {
        return tx().getConcept(getId()) == null;
    }

    protected final GrpcGrakn.TxResponse runMethod(GrpcConcept.ConceptMethod method) {
        GrpcGrakn.RunConceptMethod.Builder runConceptMethod = GrpcGrakn.RunConceptMethod.newBuilder();
        runConceptMethod.setId(ConceptBuilder.conceptId(getId()));
        runConceptMethod.setConceptMethod(method);

        GrpcGrakn.TxRequest conceptMethodRequest = GrpcGrakn.TxRequest.newBuilder().setRunConceptMethod(runConceptMethod).build();

        return tx().runConceptMethod(conceptMethodRequest);
    }

    protected final <T> T runMethod(ConceptMethod<T> method) {
        T result = tx().client().runConceptMethod(getId(), method);
        return Objects.requireNonNull(result);
    }

    final Self runVoidMethod(ConceptMethod<Void> method) {
        tx().client().runConceptMethod(getId(), method);
        return asSelf(this);
    }

    abstract Self asSelf(Concept concept);
}
