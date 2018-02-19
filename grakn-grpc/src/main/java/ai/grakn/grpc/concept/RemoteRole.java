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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc.concept;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteRole extends RemoteSchemaConcept<Role> implements Role {

    public static RemoteRole create(GraknTx tx, ConceptId id, Label label, boolean isImplicit) {
        return new AutoValue_RemoteRole(tx, id, label, isImplicit);
    }

    @Override
    public final Role sup(Role type) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Role sub(Role type) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<RelationshipType> relationshipTypes() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Type> playedByTypes() {
        throw new UnsupportedOperationException(); // TODO: implement
    }
}
