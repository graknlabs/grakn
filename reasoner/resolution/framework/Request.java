/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.framework;

import grakn.core.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.Root;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static grakn.common.collection.Collections.list;

public class Request {

    // TODO: add compute mode: single vs all (for negation vs regular)

    private final Actor<? extends Resolver<?>> sender;
    private final Actor<? extends Resolver<?>> receiver;
    private final Partial<?> partialAnswer;
    private final int planIndex;

    private final int hash;

    private Request(@Nullable Actor<? extends Resolver<?>> sender,
                    Actor<? extends Resolver<?>> receiver, Partial<?> partialAnswer,
                    int planIndex) {
        this.sender = sender;
        this.receiver = receiver;
        this.partialAnswer = partialAnswer;
        this.planIndex = planIndex;
        this.hash = Objects.hash(sender, this.partialAnswer);
    }

    public static Request create(Actor<? extends Resolver<?>> sender, Actor<? extends Resolver<?>> receiver, Partial<?> startingConcept, int planIndex) {
        return new Request(sender, receiver, startingConcept, planIndex);
    }

    public static Request create(Actor<? extends Resolver<?>> sender, Actor<? extends Resolver<?>> receiver, Partial<?> startingConcept) {
        return new Request(sender, receiver, startingConcept, -1);
    }

    public static Request create(Actor<? extends Resolver<?>> receiver, Partial<?> startingConcept) {
        return new Request(null, receiver, startingConcept, -1);
    }

    public Actor<? extends Resolver<?>> receiver() {
        return receiver;
    }

    public Actor<? extends Resolver<?>> sender() {
        return sender;
    }

    public Partial<?> partialAnswer() {
        return partialAnswer;
    }

    public int planIndex() {
        return planIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(sender, request.sender) &&
                Objects.equals(receiver, request.receiver) &&
                Objects.equals(partialAnswer, request.partialAnswer());
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "Request{" +
                "sender=" + sender +
                ", receiver=" + receiver +
                ", partial=" + partialAnswer +
                ", partialDerivation=" + partialAnswer.derivation() +
                '}';
    }

    public AnswerState.Derivation partialResolutions() {
        return partialAnswer.derivation();
    }

}
