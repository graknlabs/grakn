/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.rolePlayer;
import static java.util.stream.Collectors.joining;

/**
 * Represents the relation property (e.g. {@code ($x, $y)} or {@code (wife: $x, husband: $y)}) on a relationship.
 * This property can be queried and inserted.
 * This propert is comprised of instances of RolePlayer, which represents associations between a
 * role-player Thing and an optional Role.
 */
public class RelationProperty extends VarProperty {

    private final ImmutableMultiset<RelationProperty.RolePlayer> relationPlayers;

    public RelationProperty(ImmutableMultiset<RelationProperty.RolePlayer> relationPlayers) {
        if (relationPlayers == null) {
            throw new NullPointerException("Null relationPlayers");
        }
        this.relationPlayers = relationPlayers;
    }

    public ImmutableMultiset<RelationProperty.RolePlayer> relationPlayers() {
        return relationPlayers;
    }

    @Override
    public String getName() {
        return "relationship";
    }

    public String getProperty() {
        return "(" + relationPlayers().stream().map(Object::toString).collect(joining(", ")) + ")";
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public String toString() {
        return getProperty();
    }

    @Override
    public Stream<Statement> getTypes() {
        return relationPlayers().stream().map(RolePlayer::getRole).flatMap(CommonUtil::optionalToStream);
    }

    @Override
    public Stream<Statement> innerStatements() {
        return relationPlayers().stream().flatMap(relationPlayer -> {
            Stream.Builder<Statement> builder = Stream.builder();
            builder.add(relationPlayer.getPlayer());
            relationPlayer.getRole().ifPresent(builder::add);
            return builder.build();
        });
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Collection<Variable> castingNames = new HashSet<>();

        ImmutableSet<EquivalentFragmentSet> traversals = relationPlayers().stream().flatMap(relationPlayer -> {

            Variable castingName = Pattern.var();
            castingNames.add(castingName);

            return equivalentFragmentSetFromCasting(start, castingName, relationPlayer);
        }).collect(toImmutableSet());

        ImmutableSet<EquivalentFragmentSet> distinctCastingTraversals = castingNames.stream().flatMap(
                castingName -> castingNames.stream()
                        .filter(otherName -> !otherName.equals(castingName))
                        .map(otherName -> EquivalentFragmentSets.neq(this, castingName, otherName))
        ).collect(toImmutableSet());

        return Sets.union(traversals, distinctCastingTraversals);
    }

    private Stream<EquivalentFragmentSet> equivalentFragmentSetFromCasting(Variable start, Variable castingName, RolePlayer relationPlayer) {
        Optional<Statement> roleType = relationPlayer.getRole();

        if (roleType.isPresent()) {
            return addRelatesPattern(start, castingName, roleType.get(), relationPlayer.getPlayer());
        } else {
            return addRelatesPattern(start, castingName, relationPlayer.getPlayer());
        }
    }

    /**
     * Add some patterns where this variable is a relation and the given variable is a roleplayer of that relationship
     *
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(Variable start, Variable casting, Statement rolePlayer) {
        return Stream.of(rolePlayer(this, start, casting, rolePlayer.var(), null));
    }

    /**
     * Add some patterns where this variable is a relation relating the given roleplayer as the given roletype
     *
     * @param roleType   a variable that is the roletype of the given roleplayer
     * @param rolePlayer a variable that is a roleplayer of this relation
     */
    private Stream<EquivalentFragmentSet> addRelatesPattern(Variable start, Variable casting, Statement roleType, Statement rolePlayer) {
        return Stream.of(rolePlayer(this, start, casting, rolePlayer.var(), roleType.var()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RelationProperty) {
            RelationProperty that = (RelationProperty) o;
            return (this.relationPlayers.equals(that.relationPlayers()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.relationPlayers.hashCode();
        return h;
    }

    /**
     * A pair of role and role player (where the role may not be present)
     */
    public static class RolePlayer {

        private final Statement role;
        private final Statement player;

        public RolePlayer(@Nullable Statement role, Statement player) {
            this.role = role;
            if (player == null) {
                throw new NullPointerException("Null player");
            }
            this.player = player;
        }

        /**
         * @return the role, if specified
         */
        @CheckReturnValue
        public Optional<Statement> getRole() {
            return Optional.ofNullable(role);
        }

        /**
         * @return the role player
         */
        @CheckReturnValue
        public Statement getPlayer() {
            return player;
        }

        @Override
        public String toString() {
            return getRole().map(r -> r.getPrintableName() + ": ").orElse("") + getPlayer().getPrintableName();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof RolePlayer) {
                RolePlayer that = (RolePlayer) o;
                return (Objects.equals(this.role, that.role))
                        && (this.player.equals(that.player));
            }
            return false;
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            if (this.role != null) {
                h ^= this.role.hashCode();
            }
            h *= 1000003;
            h ^= this.player.hashCode();
            return h;
        }
    }
}
