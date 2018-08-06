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

package ai.grakn.graql.internal.query;

import ai.grakn.ComputeExecutor;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.GraqlSyntax.Char.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Char.EQUAL;
import static ai.grakn.util.GraqlSyntax.Char.QUOTE;
import static ai.grakn.util.GraqlSyntax.Char.SEMICOLON;
import static ai.grakn.util.GraqlSyntax.Char.SPACE;
import static ai.grakn.util.GraqlSyntax.Char.SQUARE_CLOSE;
import static ai.grakn.util.GraqlSyntax.Char.SQUARE_OPEN;
import static ai.grakn.util.GraqlSyntax.Command.COMPUTE;
import static ai.grakn.util.GraqlSyntax.Compute.ALGORITHMS_ACCEPTED;
import static ai.grakn.util.GraqlSyntax.Compute.ALGORITHMS_DEFAULT;
import static ai.grakn.util.GraqlSyntax.Compute.ARGUMENTS_ACCEPTED;
import static ai.grakn.util.GraqlSyntax.Compute.ARGUMENTS_DEFAULT;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm;
import static ai.grakn.util.GraqlSyntax.Compute.Argument;
import static ai.grakn.util.GraqlSyntax.Compute.CONDITIONS_ACCEPTED;
import static ai.grakn.util.GraqlSyntax.Compute.CONDITIONS_REQUIRED;
import static ai.grakn.util.GraqlSyntax.Compute.Condition;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.IN;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.OF;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.WHERE;
import static ai.grakn.util.GraqlSyntax.Compute.INCLUDE_ATTRIBUTES_DEFAULT;
import static ai.grakn.util.GraqlSyntax.Compute.Method;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.CONTAINS;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.K;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.MIN_K;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.SIZE;
import static java.util.stream.Collectors.joining;


/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn
 * @param <T> return type of ComputeQuery
 */
public class ComputeQueryImpl<T extends Answer> implements ComputeQuery<T> {

    private GraknTx tx;
    private Set<ComputeExecutor> runningJobs = ConcurrentHashMap.newKeySet();

    private Method method;
    private boolean includeAttributes;

    // All these condition properties need to start off as NULL, they will be initialised when the user provides input
    private ConceptId fromID = null;
    private ConceptId toID = null;
    private Set<Label> ofTypes = null;
    private Set<Label> inTypes = null;
    private Algorithm algorithm = null;
    private ArgumentsImpl arguments = null; // But arguments will also be set when where() is called for cluster/centrality

    private final Map<Condition, Supplier<Optional<?>>> conditionsMap = setConditionsMap();

    public ComputeQueryImpl(GraknTx tx, Method<T> method) {
        this(tx, method, INCLUDE_ATTRIBUTES_DEFAULT.get(method));
    }

    public ComputeQueryImpl(GraknTx tx, Method method, boolean includeAttributes) {
        this.method = method;
        this.tx = tx;
        this.includeAttributes = includeAttributes;
    }

    private Map<Condition, Supplier<Optional<?>>> setConditionsMap() {
        Map<Condition, Supplier<Optional<?>>> conditions = new HashMap<>();
        conditions.put(FROM, this::from);
        conditions.put(TO, this::to);
        conditions.put(OF, this::of);
        conditions.put(IN, this::in);
        conditions.put(USING, this::using);
        conditions.put(WHERE, this::where);

        return conditions;
    }

    @Override
    public final Stream<T> stream() {
        Optional<GraqlQueryException> exception = getException();
        if (exception.isPresent()) throw exception.get();

        ComputeExecutor<T> job = executor().run(this);

        runningJobs.add(job);

        try {
            return job.stream();
        } finally {
            runningJobs.remove(job);
        }

    }

    @Override
    public final void kill() {
        runningJobs.forEach(ComputeExecutor::kill);
    }

    @Override
    public final ComputeQuery<T> withTx(GraknTx tx) {
        this.tx = tx;
        return this;
    }

    @Override
    public final GraknTx tx() {
        return tx;
    }

    @Override
    public final Method method() {
        return method;
    }

    @Override
    public final ComputeQuery<T> from(ConceptId fromID) {
        this.fromID = fromID;
        return this;
    }

    @Override
    public final Optional<ConceptId> from() {
        return Optional.ofNullable(fromID);
    }

    @Override
    public final ComputeQuery<T> to(ConceptId toID) {
        this.toID = toID;
        return this;
    }

    @Override
    public final Optional<ConceptId> to() {
        return Optional.ofNullable(toID);
    }

    @Override
    public final ComputeQuery<T> of(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return of(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final ComputeQuery<T> of(Collection<Label> types) {
        this.ofTypes = ImmutableSet.copyOf(types);

        return this;
    }

    @Override
    public final Optional<Set<Label>> of() {
        return Optional.ofNullable(ofTypes);
    }

    @Override
    public final ComputeQuery<T> in(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return in(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final ComputeQuery<T> in(Collection<Label> types) {
        this.inTypes = ImmutableSet.copyOf(types);
        return this;
    }

    @Override
    public final Optional<Set<Label>> in() {
        if (this.inTypes == null) return Optional.of(ImmutableSet.of());
        return Optional.of(this.inTypes);
    }

    @Override
    public final ComputeQuery<T> using(Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    @Override
    public final Optional<Algorithm> using() {
        if (ALGORITHMS_DEFAULT.containsKey(method) && algorithm == null) return Optional.of(ALGORITHMS_DEFAULT.get(method));
        return Optional.ofNullable(algorithm);
    }

    @Override
    public final ComputeQuery<T> where(Argument arg, Argument... args) {
        ArrayList<Argument> argList = new ArrayList(args.length + 1);
        argList.add(arg);
        argList.addAll(Arrays.asList(args));

        return this.where(argList);
    }

    @Override
    public final ComputeQuery<T> where(Collection<Argument> args) {
        if (this.arguments == null) this.arguments = new ArgumentsImpl();
        for (Argument arg : args) this.arguments.setArgument(arg);

        return this;
    }

    @Override
    public final Optional<Arguments> where() {
        if (ARGUMENTS_DEFAULT.containsKey(method) && arguments == null) arguments = new ArgumentsImpl();
        return Optional.ofNullable(this.arguments);
    }

    @Override
    public final ComputeQueryImpl includeAttributes(boolean include) {
        this.includeAttributes = include;
        return this;
    }

    @Override
    public final boolean includesAttributes() {
        return includeAttributes;
    }

    @Override
    public final Boolean inferring() {
        return false;
    }

    @Override
    public final boolean isValid() {
        return !getException().isPresent();
    }

    @Override
    public Optional<GraqlQueryException> getException() {
        // Check that all required conditions for the current query method are provided
        for (Condition condition : collect(CONDITIONS_REQUIRED.get(this.method()))) {
            if (!this.conditionsMap.get(condition).get().isPresent()) {
                return Optional.of(GraqlQueryException.invalidComputeQuery_missingCondition(this.method()));
            }
        }

        // Check that all the provided conditions are accepted for the current query method
        for (Condition condition : this.conditionsMap.keySet().stream()
                .filter(con -> this.conditionsMap.get(con).get().isPresent())
                .collect(Collectors.toSet())) {
            if (!CONDITIONS_ACCEPTED.get(this.method()).contains(condition)) {
                return Optional.of(GraqlQueryException.invalidComputeQuery_invalidCondition(this.method()));
            }
        }

        // Check that the provided algorithm is accepted for the current query method
        if (ALGORITHMS_ACCEPTED.containsKey(this.method()) && !ALGORITHMS_ACCEPTED.get(this.method()).contains(this.using().get())) {
            return Optional.of(GraqlQueryException.invalidComputeQuery_invalidMethodAlgorithm(this.method()));
        }

        // Check that the provided arguments are accepted for the current query method and algorithm
        if (this.where().isPresent()) {
            for (Parameter param : this.where().get().getParameters()) {
                if (!ARGUMENTS_ACCEPTED.get(this.method()).get(this.using().get()).contains(param)) {
                    return Optional.of(GraqlQueryException.invalidComputeQuery_invalidArgument(this.method(), this.using().get()));
                }
            }
        }

        return Optional.empty();
    }

    private <T> Collection<T> collect(Collection<T> collection) {
        return collection != null ? collection : Collections.emptyList();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(str(COMPUTE, SPACE, method));
        if (!conditionsSyntax().isEmpty()) query.append(str(SPACE, conditionsSyntax()));
        query.append(SEMICOLON);

        return query.toString();
    }

    private String conditionsSyntax() {
        List<String> conditionsList = new ArrayList<>();

        // It is important that check for whether each condition is NULL, rather than using the getters.
        // Because, we want to know the user provided conditions, rather than the default conditions from the getters.
        // The exception is for arguments. It needs to be set internally for the query object to have default argument
        // values. However, we can query for .getParameters() to get user provided argument parameters.
        if (fromID != null) conditionsList.add(str(FROM, SPACE, QUOTE, fromID, QUOTE));
        if (toID != null) conditionsList.add(str(TO, SPACE, QUOTE, toID, QUOTE));
        if (ofTypes != null) conditionsList.add(ofSyntax());
        if (inTypes != null) conditionsList.add(inSyntax());
        if (algorithm != null) conditionsList.add(algorithmSyntax());
        if (arguments != null && !arguments.getParameters().isEmpty()) conditionsList.add(argumentsSyntax());

        return conditionsList.stream().collect(joining(COMMA_SPACE.toString()));
    }

    private String ofSyntax() {
        if (ofTypes != null) return str(OF, SPACE, typesSyntax(ofTypes));

        return "";
    }

    private String inSyntax() {
        if (inTypes != null) return str(IN, SPACE, typesSyntax(inTypes));

        return "";
    }

    private String typesSyntax(Set<Label> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) inTypesString.append(StringConverter.typeLabelToString(types.iterator().next()));
            else {
                inTypesString.append(SQUARE_OPEN);
                inTypesString.append(inTypes.stream().map(StringConverter::typeLabelToString).collect(joining(COMMA_SPACE.toString())));
                inTypesString.append(SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String algorithmSyntax() {
        if (algorithm != null) return str(USING, SPACE, algorithm);

        return "";
    }

    private String argumentsSyntax() {
        if (arguments == null) return "";

        List<String> argumentsList = new ArrayList<>();
        StringBuilder argumentsString = new StringBuilder();

        for (Parameter param : arguments.getParameters()) {
            argumentsList.add(str(param, EQUAL, arguments.getArgument(param).get()));
        }

        if (!argumentsList.isEmpty()) {
            argumentsString.append(str(WHERE, SPACE));
            if (argumentsList.size() == 1) argumentsString.append(argumentsList.get(0));
            else {
                argumentsString.append(SQUARE_OPEN);
                argumentsString.append(argumentsList.stream().collect(joining(COMMA_SPACE.toString())));
                argumentsString.append(SQUARE_CLOSE);
            }
        }

        return argumentsString.toString();
    }

    private String str(Object... objects) {
        StringBuilder builder = new StringBuilder();
        for (Object obj : objects) builder.append(obj.toString());
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComputeQuery<?> that = (ComputeQuery<?>) o;

        return (Objects.equals(this.tx(), that.tx()) &&
                this.method().equals(that.method()) &&
                this.from().equals(that.from()) &&
                this.to().equals(that.to()) &&
                this.of().equals(that.of()) &&
                this.in().equals(that.in()) &&
                this.using().equals(that.using()) &&
                this.where().equals(that.where()) &&
                this.includesAttributes() == that.includesAttributes());
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Objects.hashCode(method);
        result = 31 * result + Objects.hashCode(fromID);
        result = 31 * result + Objects.hashCode(toID);
        result = 31 * result + Objects.hashCode(ofTypes);
        result = 31 * result + Objects.hashCode(inTypes);
        result = 31 * result + Objects.hashCode(algorithm);
        result = 31 * result + Objects.hashCode(arguments);
        result = 31 * result + Objects.hashCode(includeAttributes);

        return result;
    }

    /**
     * Argument inner class to provide access Compute Query arguments
     *
     * @author Grakn Warriors
     */
    public class ArgumentsImpl implements Arguments {

        private LinkedHashMap<Parameter, Argument> argumentsOrdered = new LinkedHashMap<>();

        private final Map<Parameter, Supplier<Optional<?>>> argumentsMap = setArgumentsMap();

        private Map<Parameter, Supplier<Optional<?>>> setArgumentsMap() {
            Map<Parameter, Supplier<Optional<?>>> arguments = new HashMap<>();
            arguments.put(MIN_K, this::minK);
            arguments.put(K, this::k);
            arguments.put(SIZE, this::size);
            arguments.put(CONTAINS, this::contains);

            return arguments;
        }

        private void setArgument(Argument arg) {
            argumentsOrdered.remove(arg.type());
            argumentsOrdered.put(arg.type(), arg);
        }

        @Override
        public Optional<?> getArgument(Parameter param) {
            return argumentsMap.get(param).get();
        }

        @Override
        public Collection<Parameter> getParameters() {
            return argumentsOrdered.keySet();
        }

        @Override
        public Optional<Long> minK() {
            Object defaultArg = getDefaultArgument(MIN_K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(MIN_K));
        }

        @Override
        public Optional<Long> k() {
            Object defaultArg = getDefaultArgument(K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(K));
        }

        @Override
        public Optional<Long> size() {
            return Optional.ofNullable((Long) getArgumentValue(SIZE));
        }

        @Override
        public Optional<ConceptId> contains() {
            return Optional.ofNullable((ConceptId) getArgumentValue(CONTAINS));
        }

        private Object getArgumentValue(Parameter param) {
            return argumentsOrdered.get(param) != null ? argumentsOrdered.get(param).get() : null;
        }

        private Object getDefaultArgument(Parameter param) {
            if (ARGUMENTS_DEFAULT.containsKey(method) &&
                    ARGUMENTS_DEFAULT.get(method).containsKey(algorithm) &&
                    ARGUMENTS_DEFAULT.get(method).get(algorithm).containsKey(param) &&
                    !argumentsOrdered.containsKey(param))
            {
                return ARGUMENTS_DEFAULT.get(method).get(algorithm).get(param);
            }

            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArgumentsImpl that = (ArgumentsImpl) o;

            return (this.minK().equals(that.minK()) &&
                    this.k().equals(that.k()) &&
                    this.size().equals(that.size()) &&
                    this.contains().equals(that.contains()));
        }


        @Override
        public int hashCode() {
            int result = tx.hashCode();
            result = 31 * result + argumentsOrdered.hashCode();

            return result;
        }
    }
}
