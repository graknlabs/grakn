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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.optionalToStream;

abstract class AbstractFragment implements Fragment{

    // TODO: Find a better way to represent these values (either abstractly, or better estimates)
    static final long NUM_INSTANCES_PER_TYPE = 100;
    static final long NUM_INSTANCES_PER_SCOPE = 100;
    static final long NUM_RELATION_PER_CASTING = 10;
    static final long NUM_SHORTCUT_EDGES_PER_INSTANCE = 10;
    static final long NUM_SUBTYPES_PER_TYPE = 3;
    static final long NUM_CASTINGS_PER_INSTANCE = 3;
    static final long NUM_SCOPES_PER_INSTANCE = 3;
    static final long NUM_TYPES_PER_ROLE = 3;
    static final long NUM_ROLES_PER_TYPE = 3;
    static final long NUM_ROLES_PER_RELATION = 2;

    private final String start;
    private final Optional<String> end;
    private EquivalentFragmentSet equivalentFragmentSet;

    AbstractFragment(String start) {
        this.start = start;
        this.end = Optional.empty();
    }

    AbstractFragment(String start, String end) {
        this.start = start;
        this.end = Optional.of(end);
    }

    @Override
    public final EquivalentFragmentSet getEquivalentFragmentSet() {
        return equivalentFragmentSet;
    }

    @Override
    public final void setEquivalentFragmentSet(EquivalentFragmentSet equivalentFragmentSet) {
        this.equivalentFragmentSet = equivalentFragmentSet;
    }

    @Override
    public final String getStart() {
        return start;
    }

    @Override
    public final Optional<String> getEnd() {
        return end;
    }

    @Override
    public Stream<String> getVariableNames() {
        return Stream.concat(Stream.of(start), optionalToStream(end));
    }

    @Override
    public final int compareTo(@SuppressWarnings("NullableProblems") Fragment other) {
        // Don't want to use Jetbrain's @NotNull annotation
        if (this == other) return 0;
        return getPriority().compareTo(other.getPriority());
    }

    @Override
    public String toString() {
        return "$" + start + getName() + end.map(e -> "$" + e).orElse("");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractFragment that = (AbstractFragment) o;

        if (start != null ? !start.equals(that.start) : that.start != null) return false;
        if (end != null ? !end.equals(that.end) : that.end != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = start != null ? start.hashCode() : 0;
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
    }
}
