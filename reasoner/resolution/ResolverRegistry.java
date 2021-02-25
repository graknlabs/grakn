/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.core.common.exception.GraknCheckedException;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.actor.EventLoopGroup;
import grakn.core.concurrent.common.ConcurrentSet;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ConjunctionResolver;
import grakn.core.reasoner.resolution.resolver.DisjunctionResolver;
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.RetrievableResolver;
import grakn.core.reasoner.resolution.resolver.Root;
import grakn.core.reasoner.resolution.resolver.ConclusionResolver;
import grakn.core.reasoner.resolution.resolver.ConditionResolver;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Actor<ConcludableResolver>> concludableResolvers;
    private final ConcurrentMap<Rule, Actor<ConditionResolver>> ruleConditions;
    private final ConcurrentMap<Rule, Actor<ConclusionResolver>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Set<Actor<? extends Resolver<?>>> resolvers;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private final Planner planner;
    private EventLoopGroup elg;
    private boolean explanations;
    private final boolean resolutionTracing;
    private AtomicBoolean terminated;

    public ResolverRegistry(EventLoopGroup elg, Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                            ConceptManager conceptMgr, LogicManager logicMgr, boolean resolutionTracing) {
        this.elg = elg;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableResolvers = new HashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.resolvers = new ConcurrentSet<>();
        this.planner = new Planner(conceptMgr, logicMgr);
        this.terminated = new AtomicBoolean(false);
        this.resolutionTracing = resolutionTracing;
    }

    public void terminateResolvers(Throwable cause) {
        if (terminated.compareAndSet(false, true)) {
            resolvers.forEach(actor -> {
                actor.tell(r -> r.terminate(cause));
            });
        }
    }

    public Actor<Root.Conjunction> root(Conjunction conjunction, @Nullable Long offset,
                                        @Nullable Long limit, Consumer<Top> onAnswer,
                                        Consumer<Integer> onFail, Consumer<Throwable> onException) throws GraknCheckedException {
        LOG.debug("Creating Root.Conjunction for: '{}'", conjunction);
        Actor<Root.Conjunction> resolver = Actor.create(
                elg, self -> new Root.Conjunction(
                        self, conjunction, offset, limit, onAnswer, onFail, onException, resolutionRecorder, this,
                        traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor<Root.Disjunction> root(Disjunction disjunction, @Nullable Long offset,
                                        @Nullable Long limit, Consumer<Top> onAnswer,
                                        Consumer<Integer> onExhausted, Consumer<Throwable> onException) throws GraknCheckedException {
        LOG.debug("Creating Root.Disjunction for: '{}'", disjunction);
        Actor<Root.Disjunction> resolver = Actor.create(
                elg, self -> new Root.Disjunction(self, disjunction, offset, limit, onAnswer, onExhausted, onException,
                                                  resolutionRecorder, this, traversalEngine, conceptMgr, resolutionTracing)
        );
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public ResolverView.Filtered negated(Negated negated, Conjunction upstream) throws GraknCheckedException {
        LOG.debug("Creating Negation resolver for : {}", negated);
        Actor<NegationResolver> negatedResolver = Actor.create(
                elg, self -> new NegationResolver(self, negated, this, traversalEngine, conceptMgr,
                                                  resolutionRecorder, resolutionTracing)
        );
        resolvers.add(negatedResolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        Set<Variable.Retrievable> filter = filter(upstream, negated);
        return ResolverView.filtered(negatedResolver, filter);
    }

    private Set<Variable.Retrievable> filter(Conjunction scope, Negated inner) {
        return scope.variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet());
    }

    public Actor<ConditionResolver> registerCondition(Rule rule) throws GraknCheckedException {
        LOG.debug("Register retrieval for rule condition actor: '{}'", rule);
        Actor<ConditionResolver> resolver = ruleConditions.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new ConditionResolver(
                self, r, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, planner,
                resolutionTracing)));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public Actor<ConclusionResolver> registerConclusion(Rule.Conclusion conclusion) throws GraknCheckedException {
        LOG.debug("Register retrieval for rule conclusion actor: '{}'", conclusion);
        Actor<ConclusionResolver> resolver = ruleConclusions.computeIfAbsent(conclusion.rule(), (r) -> Actor.create(elg, self -> new ConclusionResolver(
                self, conclusion, this, resolutionRecorder, traversalEngine, conceptMgr, resolutionTracing)));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public ResolverView registerResolvable(Resolvable<?> resolvable) throws GraknCheckedException {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private ResolverView.Filtered registerRetrievable(Retrievable retrievable) throws GraknCheckedException {
        LOG.debug("Register RetrievableResolver: '{}'", retrievable.pattern());
        Actor<RetrievableResolver> resolver = Actor.create(elg, self -> new RetrievableResolver(
                self, retrievable, this, traversalEngine, conceptMgr, resolutionTracing));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.filtered(resolver, retrievable.retrieves());
    }

    // note: must be thread safe. We could move to a ConcurrentHashMap if we create an alpha-equivalence wrapper
    private synchronized ResolverView.Mapped registerConcludable(Concludable concludable) throws GraknCheckedException {
        LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
        for (Map.Entry<Concludable, Actor<ConcludableResolver>> c : concludableResolvers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = concludable.alphaEquals(c.getKey());
            if (alphaEquality.isValid()) {
                return ResolverView.mapped(c.getValue(), alphaEquality.asValid().idMapping());
            }
        }
        Actor<ConcludableResolver> resolver = Actor.create(elg, self ->
                new ConcludableResolver(self, concludable, resolutionRecorder, this, traversalEngine, conceptMgr,
                                        logicMgr, resolutionTracing));
        concludableResolvers.put(concludable, resolver);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.mapped(resolver, identity(concludable));
    }

    public Actor<ConjunctionResolver.Nested> nested(Conjunction conjunction) throws GraknCheckedException {
        LOG.debug("Creating Conjunction resolver for : {}", conjunction);
        Actor<ConjunctionResolver.Nested> resolver = Actor.create(
                elg, self -> new ConjunctionResolver.Nested(
                        self, conjunction, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, planner,
                        resolutionTracing)
        );
        resolvers.add(resolver);
        if (terminated.get()) throw GraknCheckedException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor<DisjunctionResolver.Nested> nested(Disjunction disjunction) {
        LOG.debug("Creating Disjunction resolver for : {}", disjunction);
        return Actor.create(
                elg, self -> new DisjunctionResolver.Nested(
                        self, disjunction, resolutionRecorder, this, traversalEngine, conceptMgr, resolutionTracing)
        );
    }

    private Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    // for testing

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }

    public static abstract class ResolverView {

        public static Mapped mapped(Actor<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new Mapped(resolver, mapping);
        }

        public static Filtered filtered(Actor<? extends Resolver<?>> resolver, Set<Variable.Retrievable> filter) {
            return new Filtered(resolver, filter);
        }

        public abstract boolean isMapped();

        public abstract boolean isFiltered();

        public Mapped asMapped() {
            throw GraknException.of(ILLEGAL_CAST, getClass(), Mapped.class);
        }

        public Filtered asFiltered() {
            throw GraknException.of(ILLEGAL_CAST, getClass(), Mapped.class);
        }

        public abstract Actor<? extends Resolver<?>> resolver();

        public static class Mapped extends ResolverView {
            private final Actor<ConcludableResolver> resolver;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            public Mapped(Actor<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
                this.resolver = resolver;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public boolean isMapped() {
                return true;
            }

            @Override
            public boolean isFiltered() {
                return false;
            }

            @Override
            public Mapped asMapped() {
                return this;
            }

            @Override
            public Actor<ConcludableResolver> resolver() {
                return resolver;
            }
        }

        public static class Filtered extends ResolverView {
            private final Actor<? extends Resolver<?>> resolver;
            private final Set<Variable.Retrievable> filter;

            public Filtered(Actor<? extends Resolver<?>> resolver, Set<Variable.Retrievable> filter) {
                this.resolver = resolver;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isMapped() {
                return false;
            }

            @Override
            public boolean isFiltered() {
                return true;
            }

            @Override
            public Filtered asFiltered() {
                return this;
            }

            @Override
            public Actor<? extends Resolver<?>> resolver() {
                return resolver;
            }
        }
    }
}
