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
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteRule extends RemoteSchemaConcept<Rule> implements Rule {

    public static RemoteRule create(GraknTx tx, ConceptId id, Label label, boolean isImplicit) {
        return new AutoValue_RemoteRule(tx, id, label, isImplicit);
    }

    @Nullable
    @Override
    public final Pattern getWhen() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Nullable
    @Override
    public final Pattern getThen() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Type> getHypothesisTypes() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Type> getConclusionTypes() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Rule sup(Rule superRule) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Rule sub(Rule type) {
        throw new UnsupportedOperationException(); // TODO: implement
    }
}
