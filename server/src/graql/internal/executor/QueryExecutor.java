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

package grakn.core.graql.internal.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.DataTypeExecutor;
import grakn.core.graql.internal.executor.property.HasAttributeExecutor;
import grakn.core.graql.internal.executor.property.HasAttributeTypeExecutor;
import grakn.core.graql.internal.executor.property.IdExecutor;
import grakn.core.graql.internal.executor.property.IsAbstractExecutor;
import grakn.core.graql.internal.executor.property.IsaAbstractExecutor;
import grakn.core.graql.internal.executor.property.LabelExecutor;
import grakn.core.graql.internal.executor.property.PlaysExecutor;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.internal.executor.property.RegexExecutor;
import grakn.core.graql.internal.executor.property.RelatesExecutor;
import grakn.core.graql.internal.executor.property.RelationExecutor;
import grakn.core.graql.internal.executor.property.SubAbstractExecutor;
import grakn.core.graql.internal.executor.property.ThenExecutor;
import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.internal.executor.property.WhenExecutor;
import grakn.core.graql.internal.gremlin.GraqlTraversal;
import grakn.core.graql.internal.gremlin.GreedyTraversalPlan;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.GroupAggregateQuery;
import grakn.core.graql.query.GroupQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubAbstractProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * QueryExecutor is the class that executes Graql queries onto the database
 */
public class QueryExecutor {

    private final boolean infer;
    private final TransactionOLTP transaction;

    public QueryExecutor(TransactionOLTP transaction, boolean infer) {
        this.infer = infer;
        this.transaction = transaction;
    }

    public Stream<ConceptMap> match(MatchClause matchClause) {
        for (Statement statement : matchClause.getPatterns().statements()) {
            statement.properties().stream().forEach(property -> validateProperty(property, statement));
        }

        if (!infer || !RuleUtils.hasRules(transaction)) {
            GraqlTraversal graqlTraversal = GreedyTraversalPlan.createTraversal(matchClause.getPatterns(), transaction);
            return traversal(matchClause.getPatterns().variables(), graqlTraversal);
        }

        try {
            Iterator<Conjunction<Statement>> conjIt = matchClause.getPatterns().getDisjunctiveNormalForm().getPatterns().iterator();
            Conjunction<Statement> conj = conjIt.next();

            ReasonerQuery conjQuery = ReasonerQueries.create(conj, transaction).rewrite();
            conjQuery.checkValid();
            Stream<ConceptMap> answerStream = conjQuery.isRuleResolvable() ?
                    conjQuery.resolve() :
                    transaction.stream(Graql.match(conj), false);

            while (conjIt.hasNext()) {
                conj = conjIt.next();
                conjQuery = ReasonerQueries.create(conj, transaction).rewrite();
                Stream<ConceptMap> localStream = conjQuery.isRuleResolvable() ?
                        conjQuery.resolve() :
                        transaction.stream(Graql.match(conj), false);

                answerStream = Stream.concat(answerStream, localStream);
            }
            return answerStream.map(result -> result.project(matchClause.getSelectedNames()));
        } catch (GraqlQueryException e) {
            System.err.println(e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * @param commonVars     set of variables of interest
     * @param graqlTraversal gral traversal corresponding to the provided pattern
     * @return resulting answer stream
     */
    public Stream<ConceptMap> traversal(Set<Variable> commonVars, GraqlTraversal graqlTraversal) {
        Set<Variable> vars = Sets.filter(commonVars, Variable::isUserDefinedName);

        GraphTraversal<Vertex, Map<String, Element>> traversal = graqlTraversal.getGraphTraversal(transaction, vars);

        return traversal.toStream()
                .map(elements -> createAnswer(vars, elements))
                .distinct()
                .sequential()
                .map(ConceptMap::new);
    }

    /**
     * @param vars     set of variables of interest
     * @param elements a map of vertices and edges where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<Variable, Concept> createAnswer(Set<Variable> vars, Map<String, Element> elements) {
        Map<Variable, Concept> map = new HashMap<>();
        for (Variable var : vars) {
            Element element = elements.get(var.symbol());
            if (element == null) {
                throw GraqlQueryException.unexpectedResult(var);
            } else {
                Concept result;
                if (element instanceof Vertex) {
                    result = transaction.buildConcept((Vertex) element);
                } else {
                    result = transaction.buildConcept((Edge) element);
                }
                Concept concept = result;
                map.put(var, concept);
            }
        }
        return map;
    }

    public ConceptMap define(DefineQuery query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(s -> s.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(definable(statement.var(), property).defineExecutors());
            }
        }
        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public ConceptMap undefine(UndefineQuery query) {
        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : allPatterns) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(definable(statement.var(), property).undefineExecutors());
            }
        }
        return WriteExecutor.create(transaction, executors.build()).write(new ConceptMap());
    }

    public Stream<ConceptMap> insert(InsertQuery query) {
        Collection<Statement> statements = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        ImmutableSet.Builder<PropertyExecutor.Writer> executors = ImmutableSet.builder();
        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()) {
                executors.addAll(insertable(statement.var(), property).insertExecutors());
            }
        }

        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> matchVars = match.getSelectedNames();
            Set<Variable> insertVars = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());

            Set<Variable> projectedVars = new HashSet<>(matchVars);
            projectedVars.retainAll(insertVars);

            Stream<ConceptMap> answers = transaction.stream(match.get(projectedVars), infer);
            return answers.map(answer -> WriteExecutor
                    .create(transaction, executors.build()).write(answer))
                    .collect(toList()).stream();
        } else {
            return Stream.of(WriteExecutor.create(transaction, executors.build()).write(new ConceptMap()));
        }
    }

    public ConceptSet delete(DeleteQuery query) {
        Stream<ConceptMap> answers = transaction.stream(query.match(), infer)
                .map(result -> result.project(query.vars()))
                .distinct();

        // TODO: We should not need to collect toSet, once we fix ConceptId.id() to not use cache.
        // Stream.distinct() will then work properly when it calls ConceptImpl.equals()
        Set<Concept> conceptsToDelete = answers.flatMap(answer -> answer.concepts().stream()).collect(toSet());
        conceptsToDelete.forEach(concept -> {
            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }
            concept.delete();
        });

        // TODO: return deleted Concepts instead of ConceptIds
        return new ConceptSet(conceptsToDelete.stream().map(Concept::id).collect(toSet()));
    }

    public Stream<ConceptMap> get(GetQuery query) {
        return match(query.match()).map(result -> result.project(query.vars())).distinct();
    }

    public Stream<Value> aggregate(AggregateQuery query) {
        Stream<ConceptMap> answers = get(query.getQuery());
        switch (query.method()) {
            case COUNT:
                return AggregateExecutor.count(answers).stream();
            case MAX:
                return AggregateExecutor.max(answers, query.var()).stream();
            case MEAN:
                return AggregateExecutor.mean(answers, query.var()).stream();
            case MEDIAN:
                return AggregateExecutor.median(answers, query.var()).stream();
            case MIN:
                return AggregateExecutor.min(answers, query.var()).stream();
            case STD:
                return AggregateExecutor.std(answers, query.var()).stream();
            case SUM:
                return AggregateExecutor.sum(answers, query.var()).stream();
            default:
                throw new IllegalArgumentException("Invalid Aggregate query method / variables");
        }
    }

    public Stream<AnswerGroup<ConceptMap>> group(GroupQuery query) {
        return group(get(query.getQuery()), query.var(),
                     answers -> answers.collect(Collectors.toList())
        ).stream();
    }

    public Stream<AnswerGroup<Value>> group(GroupAggregateQuery query) {
        return group(get(query.getQuery()), query.var(),
                     answers -> AggregateExecutor.aggregate(answers, query.aggregateMethod(), query.aggregateVar())
        ).stream();
    }

    private static <T extends Answer> List<AnswerGroup<T>> group(Stream<ConceptMap> answers, Variable var,
                                                                 Function<Stream<ConceptMap>, List<T>> function) {
        Collector<ConceptMap, ?, List<T>> applyInnerAggregate =
                collectingAndThen(toList(), list -> function.apply(list.stream()));

        List<AnswerGroup<T>> answerGroups = new ArrayList<>();
        answers.collect(groupingBy(answer -> answer.get(var), applyInnerAggregate))
                .forEach((key, values) -> answerGroups.add(new AnswerGroup<>(key, values)));

        return answerGroups;
    }

    public <T extends Answer> Stream<T> compute(ComputeQuery<T> query) {
        Optional<GraqlQueryException> exception = query.getException();
        if (exception.isPresent()) throw exception.get();

        ComputeExecutor<T> job = new ComputeExecutor<>(transaction, query);

        return job.stream();
    }

    private PropertyExecutor.Definable definable(Variable var, VarProperty property) {
        if (property instanceof SubAbstractProperty) {
            return new SubAbstractExecutor(var, (SubAbstractProperty) property);

        } else if (property instanceof DataTypeProperty) {
            return new DataTypeExecutor(var, (DataTypeProperty) property);

        } else if (property instanceof HasAttributeTypeProperty) {
            return new HasAttributeTypeExecutor(var, (HasAttributeTypeProperty) property);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof IsAbstractProperty) {
            return new IsAbstractExecutor(var, (IsAbstractProperty) property);

        } else if (property instanceof LabelProperty) {
            return new LabelExecutor(var, (LabelProperty) property);

        } else if (property instanceof PlaysProperty) {
            return new PlaysExecutor(var, (PlaysProperty) property);

        } else if (property instanceof RegexProperty) {
            return new RegexExecutor(var, (RegexProperty) property);

        } else if (property instanceof RelatesProperty) {
            return new RelatesExecutor(var, (RelatesProperty) property);

        } else if (property instanceof ThenProperty) {
            return new ThenExecutor(var, (ThenProperty) property);

        } else if (property instanceof WhenProperty) {
            return new WhenExecutor(var, (WhenProperty) property);

        } else {
            throw GraqlQueryException.defineUnsupportedProperty(property.getName());
        }
    }

    private PropertyExecutor.Insertable insertable(Variable var, VarProperty property) {
        if (property instanceof IsaAbstractProperty) {
            return new IsaAbstractExecutor(var, (IsaAbstractProperty) property);

        } else if (property instanceof HasAttributeProperty) {
            return new HasAttributeExecutor(var, (HasAttributeProperty) property);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof LabelProperty) {
            return new LabelExecutor(var, (LabelProperty) property);

        } else if (property instanceof RelationProperty) {
            return new RelationExecutor(var, (RelationProperty) property);

        } else if (property instanceof ValueProperty) {
            return new ValueExecutor(var, (ValueProperty) property);

        } else {
            throw GraqlQueryException.insertUnsupportedProperty(property.getName());
        }
    }

    private void validateProperty(VarProperty varProperty, Statement statement) {
        if (varProperty instanceof IsaAbstractProperty) {
            validateIsaProperty((IsaAbstractProperty) varProperty);
        } else if (varProperty instanceof HasAttributeProperty) {
            validateHasAttributeProperty((HasAttributeProperty) varProperty);
        } else if (varProperty instanceof RelationProperty) {
            validateRelationshipProperty((RelationProperty) varProperty, statement);
        }

        varProperty.innerStatements()
                .map(Statement::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .forEach(label -> {
                    if (transaction.getSchemaConcept(label) == null) {
                        throw GraqlQueryException.labelNotFound(label);
                    }
                });
    }

    private void validateIsaProperty(IsaAbstractProperty varProperty) {
        varProperty.type().getTypeLabel().ifPresent(typeLabel -> {
            SchemaConcept theSchemaConcept = transaction.getSchemaConcept(typeLabel);
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(typeLabel);
            }
        });
    }

    private void validateHasAttributeProperty(HasAttributeProperty varProperty) {
        SchemaConcept schemaConcept = transaction.getSchemaConcept(varProperty.type());
        if (schemaConcept == null) {
            throw GraqlQueryException.labelNotFound(varProperty.type());
        }
        if (!schemaConcept.isAttributeType()) {
            throw GraqlQueryException.mustBeAttributeType(varProperty.type());
        }
    }

    private void validateRelationshipProperty(RelationProperty varProperty, Statement statement) {
        Set<Label> roleTypes = varProperty.relationPlayers().stream()
                .map(RelationProperty.RolePlayer::getRole).flatMap(CommonUtil::optionalToStream)
                .map(Statement::getTypeLabel).flatMap(CommonUtil::optionalToStream)
                .collect(toSet());

        Optional<Label> maybeLabel =
                statement.getProperty(IsaProperty.class).map(IsaProperty::type).flatMap(Statement::getTypeLabel);

        maybeLabel.ifPresent(label -> {
            SchemaConcept schemaConcept = transaction.getSchemaConcept(label);

            if (schemaConcept == null || !schemaConcept.isRelationshipType()) {
                throw GraqlQueryException.notARelationType(label);
            }
        });

        // Check all role types exist
        roleTypes.forEach(roleId -> {
            SchemaConcept schemaConcept = transaction.getSchemaConcept(roleId);
            if (schemaConcept == null || !schemaConcept.isRole()) {
                throw GraqlQueryException.notARoleType(roleId);
            }
        });
    }
}
