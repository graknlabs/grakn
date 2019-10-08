/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.kb.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.kb.reasoner.atom.Atomic;
import grakn.core.kb.reasoner.query.ReasonerQuery;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import grakn.core.kb.planning.EquivalentFragmentSet;
import grakn.core.kb.executor.WriteExecutor;

import java.util.Set;

// TODO: The exceptions in this class needs to be tidied up
public interface PropertyExecutor {

    Set<EquivalentFragmentSet> matchFragments();

    Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements);

    interface Referrable extends Definable, Insertable {

        @Override
        default Set<Writer> defineExecutors() {
            return ImmutableSet.of(referrer());
        }

        @Override
        default Set<Writer> undefineExecutors() {
            return ImmutableSet.of(referrer());
        }

        @Override
        default Set<Writer> insertExecutors() {
            return ImmutableSet.of(referrer());
        }

        Referrer referrer();
    }

    interface Definable extends PropertyExecutor {

        Set<Writer> defineExecutors();

        Set<Writer> undefineExecutors();
    }

    interface Insertable extends PropertyExecutor {

        Set<Writer> insertExecutors();
    }

    interface Writer {

        Variable var();

        VarProperty property();

        Set<Variable> requiredVars();

        Set<Variable> producedVars();

        void execute(WriteExecutor executor);
    }

    interface Referrer extends Writer{

        @Override
        default Set<Variable> requiredVars() {
            return ImmutableSet.of();
        }

        @Override
        default Set<Variable> producedVars() {
            return ImmutableSet.of(var());
        }
    }

}
