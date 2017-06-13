/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPatternBuilder;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.shortcut;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getUserDefinedIdPredicate;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Represents the relation property (e.g. {@code ($x, $y)} or {@code (wife: $x, husband: $y)}) on a {@link Relation}.
 *
 * This property can be queried and inserted.
 *
 * This propert is comprised of instances of {@link RelationPlayer}, which represents associations between a
 * role-player {@link Instance} and an optional {@link RoleType}.
 *
 * @author Felix Chapman
 */
public class RelationProperty extends AbstractVarProperty implements UniqueVarProperty {

    private final ImmutableMultiset<RelationPlayer> relationPlayers;

    public RelationProperty(ImmutableMultiset<RelationPlayer> relationPlayers) {
        this.relationPlayers = relationPlayers;
    }

    public Stream<RelationPlayer> getRelationPlayers() {
        return relationPlayers.stream();
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("(").append(relationPlayers.stream().map(Object::toString).collect(joining(", "))).append(")");
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        Collection<Var> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals = relationPlayers.stream().flatMap(relationPlayer -> {

            Var castingName = Graql.var();
            castingNames.add(castingName);

            return equivalentFragmentSetFromCasting(start, castingName, relationPlayer);
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(castingName, otherName))
        ).collect(toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return relationPlayers.stream().map(RelationPlayer::getRoleType).flatMap(CommonUtil::optionalToStream);
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return relationPlayers.stream().flatMap(relationPlayer -> {
            Stream.Builder<VarPatternAdmin> builder = Stream.builder();
            builder.add(relationPlayer.getRolePlayer());
            relationPlayer.getRoleType().ifPresent(builder::add);
            return builder.build();
        });
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSetFromCasting(Var start, Var castingName, RelationPlayer relationPlayer) {
        Optional<VarPatternAdmin> roleType = relationPlayer.getRoleType();

        if (roleType.isPresent()) {
            return addRelatesPattern(start, castingName, roleType.get(), relationPlayer.getRolePlayer());
        } else {
            return addRelatesPattern(start, castingName, relationPlayer.getRolePlayer());
        }
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relation
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(Var start, Var casting, VarPatternAdmin rolePlayer) {
        return Stream.of(shortcut(start, casting, rolePlayer.getVarName(), Optional.empty()));
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     * @param roleType a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(Var start, Var casting, VarPatternAdmin roleType, VarPatternAdmin rolePlayer) {
        return Stream.of(shortcut(start, casting, rolePlayer.getVarName(), Optional.of(roleType.getVarName())));
    }

    @Override
    public void checkValidProperty(GraknGraph graph, VarPatternAdmin var) throws GraqlQueryException {

        Set<TypeLabel> roleTypes = relationPlayers.stream()
                .map(RelationPlayer::getRoleType).flatMap(CommonUtil::optionalToStream)
                .map(VarPatternAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());

        Optional<TypeLabel> maybeLabel =
                var.getProperty(IsaProperty.class).map(IsaProperty::getType).flatMap(VarPatternAdmin::getTypeLabel);

        maybeLabel.ifPresent(label -> {
            Type type = graph.getType(label);

            if (type == null || !type.isRelationType()) {
                throw GraqlQueryException.notARelationType(label);
            }
        });

        // Check all role types exist
        roleTypes.forEach(roleId -> {
            Type type = graph.getType(roleId);
            if (type == null || !type.isRoleType()) {
                throw GraqlQueryException.notARoleType(roleId);
            }
        });
    }

    @Override
    public void checkInsertable(VarPatternAdmin var) throws GraqlQueryException {
        if (!var.hasProperty(IsaProperty.class)) {
            throw GraqlQueryException.insertRelationWithoutType();
        }
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws GraqlQueryException {
        Relation relation = concept.asRelation();
        relationPlayers.forEach(relationPlayer -> addRoleplayer(insertQueryExecutor, relation, relationPlayer));
    }

    /**
     * Add a roleplayer to the given relation
     * @param relation the concept representing the relation
     * @param relationPlayer a casting between a role type and role player
     */
    private void addRoleplayer(InsertQueryExecutor insertQueryExecutor, Relation relation, RelationPlayer relationPlayer) {
        VarPatternAdmin roleVar = relationPlayer.getRoleType().orElseThrow(GraqlQueryException::insertRolePlayerWithoutRoleType);

        RoleType roleType = insertQueryExecutor.getConcept(roleVar).asRoleType();
        Instance roleplayer = insertQueryExecutor.getConcept(relationPlayer.getRolePlayer()).asInstance();
        relation.addRolePlayer(roleType, roleplayer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationProperty that = (RelationProperty) o;

        return relationPlayers.equals(that.relationPlayers);

    }

    @Override
    public int hashCode() {
        return relationPlayers.hashCode();
    }


    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //keep varName if reified, reified if contains more properties than the RelationProperty itself and potential IsaProperty
        boolean isReified = var.getProperties()
                .filter(prop -> !RelationProperty.class.isInstance(prop))
                .filter(prop -> !IsaProperty.class.isInstance(prop))
                .count() > 0;
        VarPatternBuilder relVar = (var.getVarName().isUserDefinedName() || isReified)? var.getVarName().asUserDefined() : Graql.var();
        Set<RelationPlayer> relationPlayers = this.getRelationPlayers().collect(toSet());

        for (RelationPlayer rp : relationPlayers) {
            VarPatternAdmin role = rp.getRoleType().orElse(null);
            VarPatternAdmin rolePlayer = rp.getRolePlayer();
            if (role != null) relVar = relVar.rel(role, rolePlayer);
            else relVar = relVar.rel(rolePlayer);
        }

        //id part
        IsaProperty isaProp = var.getProperty(IsaProperty.class).orElse(null);
        IdPredicate predicate = null;
        //Isa present
        if (isaProp != null) {
            VarPatternAdmin isaVar = isaProp.getType();
            TypeLabel typeLabel = isaVar.getTypeLabel().orElse(null);
            Var typeVariable = typeLabel == null ? isaVar.getVarName() : Graql.var().asUserDefined();
            relVar = relVar.isa(typeVariable);
            if (typeLabel != null) {
                GraknGraph graph = parent.graph();
                VarPatternAdmin idVar = typeVariable.id(graph.getType(typeLabel).getId()).admin();
                predicate = new IdPredicate(idVar, parent);
            } else {
                predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
            }
        }
        return new ai.grakn.graql.internal.reasoner.atom.binary.Relation(relVar.pattern().admin(), predicate, parent);
    }

}
