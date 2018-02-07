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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.internal.query.AbstractExecutableQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

abstract class AbstractComputeQuery<T, V extends ComputeQuery<T>>
        extends AbstractExecutableQuery<T> implements ComputeQuery<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(ComputeQuery.class);

    private Optional<GraknTx> tx;
    private GraknComputer graknComputer = null;
    private boolean includeAttribute;
    private ImmutableSet<Label> subLabels = ImmutableSet.of();

    private static final boolean DEFAULT_INCLUDE_ATTRIBUTE = false;

    AbstractComputeQuery(Optional<GraknTx> tx) {
        this(tx, DEFAULT_INCLUDE_ATTRIBUTE);
    }

    AbstractComputeQuery(Optional<GraknTx> tx, boolean includeAttribute) {
        this.tx = tx;
        this.includeAttribute = includeAttribute;
    }

    @Override
    public final T execute() {
        GraknTx tx = tx().orElseThrow(GraqlQueryException::noTx);

        LOGGER.info(toString() + " started");
        long startTime = System.currentTimeMillis();

        // TODO: is this definitely the right behaviour if the computer is already present?
        if (graknComputer == null) {
            graknComputer = tx.session().getGraphComputer();
        }

        T result = innerExecute(tx, graknComputer);

        LOGGER.info(toString() + " finished in " + (System.currentTimeMillis() - startTime) + " ms");

        return result;
    }

    protected abstract T innerExecute(GraknTx tx, GraknComputer computer);

    @Override
    public final Optional<GraknTx> tx() {
        return tx;
    }

    @Override
    public final V withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return (V) this;
    }

    @Override
    public final V in(String... subTypeLabels) {
        return in(Arrays.stream(subTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final V in(Collection<Label> subLabels) {
        this.subLabels = ImmutableSet.copyOf(subLabels);
        return (V) this;
    }

    @Override
    public final ImmutableSet<Label> subLabels() {
        return subLabels;
    }

    final ImmutableSet<Label> subLabels(GraknTx tx) {
        return subTypes(tx).map(SchemaConcept::getLabel).collect(toImmutableSet());
    }

    @Override
    public final V includeAttribute() {
        this.includeAttribute = true;
        return (V) this;
    }

    @Override
    public final boolean isAttributeIncluded() {
        return includeAttribute || isStatisticsQuery() || subTypesContainsImplicitOrAttributeTypes();
    }

    @Override
    public final void kill() {
        if (graknComputer != null) {
            graknComputer.killJobs();
        }
    }

    private boolean subTypesContainsImplicitOrAttributeTypes() {
        if (!tx.isPresent()) {
            return false;
        }

        GraknTx theTx = tx.get();

        return subLabels.stream().anyMatch(label -> {
            SchemaConcept type = theTx.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    final boolean selectedTypesHaveInstance(GraknTx tx) {
        if (subLabels(tx).isEmpty()) {
            LOGGER.info("No types found while looking for instances");
            return false;
        }

        List<Pattern> checkSubtypes = subLabels(tx).stream()
                .map(type -> var("x").isa(Graql.label(type))).collect(toList());
        return tx.graql().infer(false).match(or(checkSubtypes)).iterator().hasNext();
    }

    final boolean verticesExistInSubgraph(GraknTx tx, ConceptId... ids) {
        for (ConceptId id : ids) {
            Thing thing = tx.getConcept(id);
            if (thing == null || !subLabels(tx).contains(thing.type().getLabel())) return false;
        }
        return true;
    }

    abstract String graqlString();

    final String subtypeString() {
        return subLabels.isEmpty() ? ";" : " in "
                + subLabels.stream().map(StringConverter::typeLabelToString).collect(joining(", ")) + ";";
    }

    @Override
    public final String toString() {
        return "compute " + graqlString();
    }

    final Stream<Type> subTypes(GraknTx tx) {
        // get all types if subGraph is empty, else get all subTypes of each type in subGraph
        // only include attributes and implicit "has-xxx" relationships when user specifically asked for them
        if (subLabels.isEmpty()) {
            ImmutableSet.Builder<Type> subTypesBuilder = ImmutableSet.builder();

            if (isAttributeIncluded()) {
                tx.admin().getMetaConcept().subs().forEach(subTypesBuilder::add);
            } else {
                tx.admin().getMetaEntityType().subs().forEach(subTypesBuilder::add);
                tx.admin().getMetaRelationType().subs()
                        .filter(relationshipType -> !relationshipType.isImplicit()).forEach(subTypesBuilder::add);
            }

            return subTypesBuilder.build().stream();
        } else {
            Stream<Type> subTypes = subLabels.stream().map(label -> {
                Type type = tx.getType(label);
                if (type == null) throw GraqlQueryException.labelNotFound(label);
                return type;
            }).flatMap(Type::subs);

            if (!isAttributeIncluded()) {
                subTypes = subTypes.filter(relationshipType -> !relationshipType.isImplicit());
            }

            return subTypes;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractComputeQuery<?, ?> that = (AbstractComputeQuery<?, ?>) o;

        return tx.equals(that.tx) && includeAttribute == that.includeAttribute && subLabels.equals(that.subLabels);
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        result = 31 * result + subLabels.hashCode();
        return result;
    }

    final Set<LabelId> convertLabelsToIds(GraknTx tx, Set<Label> labelSet) {
        return labelSet.stream()
                .map(tx.admin()::convertToId)
                .filter(LabelId::isValid)
                .collect(Collectors.toSet());
    }
}
