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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.Rule;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Base class for defining ontological {@link Atom} - ones referring to ontological elements.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class OntologicalAtom extends TypeAtom {

    protected OntologicalAtom(VarPattern pattern, Var predicateVar, @Nullable IdPredicate p, ReasonerQuery par) {
        super(pattern, predicateVar, p, par);}
    protected OntologicalAtom(TypeAtom a) { super(a);}

    @Override
    public boolean isSelectable() {
        return true;
    }

    @Override
    public Stream<InferenceRule> getApplicableRules() { return Stream.empty();}

    @Override
    public Set<String> validateAsRuleHead(Rule rule) {
        return Sets.newHashSet(ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD.getMessage(rule.getThen(), rule.getLabel()));
    }
}
