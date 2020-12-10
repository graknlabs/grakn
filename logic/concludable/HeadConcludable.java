/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.logic.concludable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class HeadConcludable<CONSTRAINT extends Constraint, U extends HeadConcludable<CONSTRAINT, U>>
        extends Concludable<CONSTRAINT, U> {

    private HeadConcludable(CONSTRAINT constraint, Set<Variable> constraintContext) {
        super(constraint);
        copyAdditionalConstraints(constraintContext, new HashSet<>(this.constraint.variables()));
    }

    public static HeadConcludable<?, ?> of(ThingConstraint constraint, Set<Variable> constraintContext) {
        if (constraint.isRelation()) return Relation.copyOf(constraint.asRelation(), constraintContext);
        else if (constraint.isHas()) return Has.copyOf(constraint.asHas(), constraintContext);
        else if (constraint.isIsa()) return Isa.copyOf(constraint.asIsa(), constraintContext);
        else if (constraint.isValue()) return Value.copyOf(constraint.asValue(), constraintContext);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public boolean isRelation() {
        return false;
    }

    public boolean isHas() {
        return false;
    }

    public boolean isIsa() {
        return false;
    }

    public boolean isValue() {
        return false;
    }

    public Relation asRelation() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
    }

    public Has asHas() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
    }

    public Isa asIsa() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
    }

    public Value asValue() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
    }

    private void copyAdditionalConstraints(Set<Variable> fromVars, Set<Variable> toVars) {
        Map<Variable, Variable> nonAnonFromVarsMap = fromVars.stream()
                .filter(variable -> !variable.identifier().reference().isAnonymous())
                .collect(Collectors.toMap(e -> e, e -> e)); // Create a map for efficient lookups
        toVars.stream().filter(variable -> !variable.identifier().reference().isAnonymous())
                .forEach(copyTo -> {
                    if (nonAnonFromVarsMap.containsKey(copyTo)) {
                        Variable copyFrom = nonAnonFromVarsMap.get(copyTo);
                        if (copyTo.isThing() && copyFrom.isThing()) {
                            copyIsaAndValues(copyFrom.asThing(), copyTo.asThing());
                        } else if (copyTo.isType() && copyFrom.isType()) {
                            copyLabelAndValueType(copyFrom.asType(), copyTo.asType());
                        } else throw GraknException.of(ILLEGAL_STATE);
                    }
                });
    }

    public static class Relation extends HeadConcludable<RelationConstraint, Relation> {

        public Relation(RelationConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Relation copyOf(RelationConstraint constraint, Set<Variable> constraintContext) {
            return new Relation(copyConstraint(constraint), constraintContext);
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public Relation asRelation() {
            return this;
        }
    }

    public static class Has extends HeadConcludable<HasConstraint, Has> {

        public Has(HasConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Has copyOf(HasConstraint constraint, Set<Variable> constraintContext) {
            return new Has(copyConstraint(constraint), constraintContext);
        }

        private static Has of(HasConstraint constraint) {
            return new Has(constraint, constraint.variables());
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public Has asHas() {
            return this;
        }
    }

    public static class Isa extends HeadConcludable<IsaConstraint, HeadConcludable.Isa> {

        public Isa(IsaConstraint constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Isa copyOf(IsaConstraint constraint, Set<Variable> constraintContext) {
            return new Isa(copyConstraint(constraint), constraintContext);
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public Isa asIsa() {
            return this;
        }
    }

    public static class Value extends HeadConcludable<ValueConstraint<?>, Value> {

        Value(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            super(constraint, constraintContext);
        }

        public static Value copyOf(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
            return new Value(copyConstraint(constraint), constraintContext);
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
        }
    }

}

