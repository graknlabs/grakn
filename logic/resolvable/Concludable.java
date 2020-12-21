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

package grakn.core.logic.resolvable;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.logic.Rule;
import grakn.core.logic.tool.ConstraintCopier;
import grakn.core.logic.transformer.Unifier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.traversal.common.Identifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Concludable<CONSTRAINT extends Constraint> extends Resolvable {

    private final Map<Rule, Set<Unifier>> applicableRules;
    private final CONSTRAINT constraint;

    private Concludable(CONSTRAINT constraint) {
        this.constraint = constraint;
        applicableRules = new HashMap<>(); // TODO Implement
    }

    public CONSTRAINT constraint() {
        return constraint;
    }

    public static Set<Concludable<?>> create(grakn.core.pattern.Conjunction conjunction) {
        return new Extractor(conjunction.variables()).concludables();
    }

    public Stream<Pair<Rule, Unifier>> findUnifiableRules(Stream<Rule> allRules) {
        // TODO Get rules internally
        return allRules.flatMap(rule -> rule.possibleConclusions().stream()
                .flatMap(this::unify).map(variableMapping -> new Pair<>(rule, variableMapping))
        );
    }

    public Stream<Unifier> getUnifiers(Rule rule) {
        return applicableRules.get(rule).stream();
    }

    public Stream<Rule> getApplicableRules() {
        return applicableRules.keySet().stream();
    }

    private Stream<Unifier> unify(Rule.Conclusion<?> unifyWith) {
        if (unifyWith.isRelation()) return unify(unifyWith.asRelation());
        else if (unifyWith.isHas()) return unify(unifyWith.asHas());
        else if (unifyWith.isIsa()) return unify(unifyWith.asIsa());
        else if (unifyWith.isValue()) return unify(unifyWith.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Stream<Unifier> unify(Rule.Conclusion.Relation unifyWith) {
        return Stream.empty();
    }

    Stream<Unifier> unify(Rule.Conclusion.Has unifyWith) {
        return Stream.empty();
    }

    Stream<Unifier> unify(Rule.Conclusion.Isa unifyWith) {
        return Stream.empty();
    }

    Stream<Unifier> unify(Rule.Conclusion.Value unifyWith) {
        return Stream.empty();
    }

    public AlphaEquivalence alphaEquals(Concludable<?> that) {
        if (that.isRelation()) return alphaEquals(that.asRelation());
        else if (that.isHas()) return alphaEquals(that.asHas());
        else if (that.isIsa()) return alphaEquals(that.asIsa());
        else if (that.isValue()) return alphaEquals(that.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    AlphaEquivalence alphaEquals(Concludable.Relation that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Has that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Isa that) { return null; }

    AlphaEquivalence alphaEquals(Concludable.Value that) { return null; }

    public boolean isRelation() { return false; }

    public boolean isHas() { return false; }

    public boolean isIsa() { return false; }

    public boolean isValue() { return false; }

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

    Optional<Unifier> tryExtendUnifier(Variable conjVar, Variable headVar, Unifier unifier) {
        if (ConstraintCopier.varHintsDisjoint(headVar, conjVar)) return Optional.empty();
        // TODO compute set of types
        Set<Label> allowedTypes = null; // may be able to use conjVar.resolvedTypes(), if they exist. But if not?
        Unifier clone = unifier.extend(conjVar.reference().asName(), headVar.reference().asName(), allowedTypes);
        return Optional.of(clone);
    }

    <T, V> Map<T, Set<V>> cloneMapping(Map<T, Set<V>> mapping) {
        Map<T, Set<V>> clone = new HashMap<>();
        mapping.forEach((key, set) -> clone.put(key, new HashSet<>(set)));
        return clone;
    }

    public Conjunction conjunction() {
        return null; //TODO Make abstract and implement for all subtypes
    }

    public static class Relation extends Concludable<RelationConstraint> {

        public Relation(final RelationConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        public Stream<Unifier> unify(Rule.Conclusion.Relation unifyWith, ConceptManager conceptMgr) {
            if (this.constraint().players().size() > unifyWith.constraint().players().size()) return Stream.empty();
            Unifier.Builder unifierBuilder = Unifier.empty().builder();

            if (!constraint().owner().reference().isAnonymous()) {
                assert constraint().owner().reference().isName();
                if (!impossibleUnification(constraint().owner(), unifyWith.constraint().owner())) {
                    unifierBuilder.add(constraint().owner().identifier(), unifyWith.constraint().owner().identifier());
                } else return Stream.empty();
            }

            if (constraint().owner().isa().isPresent()) {
                assert unifyWith.constraint().owner().isa().isPresent(); // due to known shapes of rule conclusions
                TypeVariable relationType = constraint().owner().isa().get().type();
                TypeVariable unifyWithRelationType = unifyWith.constraint().owner().isa().get().type();
                if (!impossibleUnification(relationType, unifyWithRelationType)) {
                    unifierBuilder.add(relationType.identifier(), unifyWithRelationType.identifier());

                    if (relationType.reference().isLabel()) {
                        // require the unification target type variable satisfies a set of labels
                        Set<Label> allowedTypes = relationType.resolvedTypes().stream().flatMap(label -> {
                            if (label.scope().isPresent()) {
                                return conceptMgr.getRelationType(label.scope().get()).getSubtypes()
                                        .map(relType -> relType.getRelates(label.name())).filter(Objects::nonNull)
                                        .map(RoleType::getLabel);
                            } else {
                                return conceptMgr.getThingType(label.name()).getSubtypes().map(ThingType::getLabel);
                            }
                        }).collect(Collectors.toSet());
                        unifierBuilder.requirements().types(relationType.identifier(), allowedTypes);
                    }
                } else return Stream.empty();
            }

            List<RolePlayer> conjRolePlayers = Collections.unmodifiableList(constraint().players());
            List<RolePlayer> thenRolePlayers = Collections.unmodifiableList(unifyWith.constraint().players());

            return matchRolePlayerIndices(conjRolePlayers, thenRolePlayers, new HashMap<>())
                    .map(indexMap -> rolePlayerMappingToUnifier(indexMap, thenRolePlayers, unifierBuilder, conceptMgr));
        }

        private Stream<Map<RolePlayer, Set<Integer>>> matchRolePlayerIndices(
                List<RolePlayer> conjRolePlayers, List<RolePlayer> thenRolePlayers,
                Map<RolePlayer, Set<Integer>> mapping) {

            if (conjRolePlayers.isEmpty()) return Stream.of(mapping);
            RolePlayer conjRP = conjRolePlayers.get(0);

            return IntStream.range(0, thenRolePlayers.size())
                    .filter(thenIdx -> mapping.values().stream().noneMatch(players -> players.contains(thenIdx)))
                    .filter(thenIdx -> !impossibleUnification(conjRP, thenRolePlayers.get(thenIdx)))
                    .mapToObj(thenIdx -> {
                        Map<RolePlayer, Set<Integer>> clone = cloneMapping(mapping);
                        clone.putIfAbsent(conjRP, new HashSet<>());
                        clone.get(conjRP).add(thenIdx);
                        return clone;
                    }).flatMap(newMapping -> matchRolePlayerIndices(conjRolePlayers.subList(1, conjRolePlayers.size()),
                                                                    thenRolePlayers, newMapping));
        }

        private boolean impossibleUnification(TypeVariable typeVar, TypeVariable unifyWithTypeVar) {
            // TODO
            return false;
        }

        private boolean impossibleUnification(ThingVariable thingVar, ThingVariable unifyWithThingVar) {
            // TODO
            return false;
        }

        private boolean impossibleUnification(RolePlayer conjRP, RolePlayer unifyWithRolePlayer) {
            // TODO
            return false;
        }

        private Unifier rolePlayerMappingToUnifier(
                Map<RolePlayer, Set<Integer>> matchedRolePlayerIndices, List<RolePlayer> thenRolePlayers,
                Unifier.Builder unifierBuilder, ConceptManager conceptMgr) {

            matchedRolePlayerIndices.forEach((conjRP, thenRPIndices) -> thenRPIndices.stream().map(thenRolePlayers::get)
                    .forEach(thenRP -> {
                                 if (conjRP.roleType().isPresent()) {
                                     assert thenRP.roleType().isPresent();
                                     TypeVariable roleTypeVar = conjRP.roleType().get();
                                     unifierBuilder.add(roleTypeVar.identifier(), thenRP.roleType().get().identifier());

                                     if (roleTypeVar.reference().isLabel()) {
                                         Set<Label> allowedTypes = roleTypeVar.resolvedTypes().stream().flatMap(roleLabel -> {
                                             assert roleLabel.scope().isPresent();
                                             return conceptMgr.getRelationType(roleLabel.scope().get()).getSubtypes()
                                                     .map(relType -> relType.getRelates(roleLabel.name()))
                                                     .filter(Objects::nonNull).map(Type::getLabel);
                                         }).collect(Collectors.toSet());
                                         unifierBuilder.requirements().types(roleTypeVar.identifier(), allowedTypes);
                                     }
                                 }
                                 unifierBuilder.add(conjRP.player().identifier(), thenRP.player().identifier());
                             }
                    ));
            return unifierBuilder.build();
        }

        @Override
        public boolean isRelation() {
            return true;
        }

        @Override
        public Relation asRelation() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Concludable.Relation that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Has extends Concludable<HasConstraint> {

        public Has(final HasConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        public Stream<Unifier> unify(Rule.Conclusion.Has unifyWith) {
            Optional<Unifier> unifier = tryExtendUnifier(constraint().owner(), unifyWith.constraint().owner(), Unifier.empty());
            if (constraint().attribute().reference().isName()) {
                unifier = unifier.flatMap(u -> tryExtendUnifier(constraint().attribute(), unifyWith.constraint().attribute(), u));
            }
            return unifier.map(Stream::of).orElseGet(Stream::empty);
        }

        @Override
        public boolean isHas() {
            return true;
        }

        @Override
        public Has asHas() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Concludable.Has that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Isa extends Concludable<IsaConstraint> {

        public Isa(final IsaConstraint constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        Stream<Unifier> unify(Rule.Conclusion.Isa unifyWith) {
            Optional<Unifier> unifier = tryExtendUnifier(constraint().owner(), unifyWith.constraint().owner(), Unifier.empty());
            if (constraint().type().reference().isName()) {
                unifier = unifier.flatMap(u -> tryExtendUnifier(constraint().type(), unifyWith.constraint().type(), u));
            }
            return unifier.map(Stream::of).orElseGet(Stream::empty);
        }

        @Override
        public boolean isIsa() {
            return true;
        }

        @Override
        public Isa asIsa() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Concludable.Isa that) {
            return constraint().alphaEquals(that.constraint());
        }
    }

    public static class Value extends Concludable<ValueConstraint<?>> {

        public Value(final ValueConstraint<?> constraint) {
            super(ConstraintCopier.copyConstraint(constraint));
        }

        @Override
        Stream<Unifier> unify(Rule.Conclusion.Value unifyWith) {
            Optional<Unifier> unifier = tryExtendUnifier(constraint().owner(), unifyWith.constraint().owner(), Unifier.empty());
            if (constraint().isVariable() && constraint().asVariable().value().reference().isName()) {
                unifier = unifier.flatMap(u -> tryExtendUnifier(constraint().asVariable().value(),
                                                                unifyWith.constraint().asVariable().value(), u));
            }
            return unifier.map(Stream::of).orElseGet(Stream::empty);
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
        }

        @Override
        AlphaEquivalence alphaEquals(Concludable.Value that) {
            return constraint().alphaEquals(that.constraint());
        }
    }


    private static class Extractor {

        private final Set<Variable> isaOwnersToSkip = new HashSet<>();
        private final Set<Variable> valueOwnersToSkip = new HashSet<>();
        private final Set<Concludable<?>> concludables = new HashSet<>();

        Extractor(Set<Variable> variables) {
            Set<Constraint> constraints = variables.stream().flatMap(variable -> variable.constraints().stream())
                    .collect(Collectors.toSet());
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isRelation)
                    .map(ThingConstraint::asRelation).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isHas)
                    .map(ThingConstraint::asHas).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isIsa)
                    .map(ThingConstraint::asIsa).forEach(this::fromConstraint);
            constraints.stream().filter(Constraint::isThing).map(Constraint::asThing).filter(ThingConstraint::isValue)
                    .map(ThingConstraint::asValue).forEach(this::fromConstraint);
        }

        public void fromConstraint(RelationConstraint relationConstraint) {
            concludables.add(new Concludable.Relation(relationConstraint));
            isaOwnersToSkip.add(relationConstraint.owner());
        }

        private void fromConstraint(HasConstraint hasConstraint) {
            concludables.add(new Concludable.Has(hasConstraint));
            isaOwnersToSkip.add(hasConstraint.attribute());
            if (hasConstraint.attribute().isa().isPresent()) valueOwnersToSkip.add(hasConstraint.attribute());
        }

        public void fromConstraint(IsaConstraint isaConstraint) {
            if (isaOwnersToSkip.contains(isaConstraint.owner())) return;
            concludables.add(new Concludable.Isa(isaConstraint));
            isaOwnersToSkip.add(isaConstraint.owner());
            valueOwnersToSkip.add(isaConstraint.owner());
        }

        private void fromConstraint(ValueConstraint<?> valueConstraint) {
            if (valueOwnersToSkip.contains(valueConstraint.owner())) return;
            concludables.add(new Concludable.Value(valueConstraint));
        }

        public Set<Concludable<?>> concludables() {
            return new HashSet<>(concludables);
        }
    }
}
