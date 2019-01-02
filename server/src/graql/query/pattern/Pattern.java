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

package grakn.core.graql.query.pattern;

import grakn.core.graql.concept.Label;
import grakn.core.graql.parser.Parser;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A pattern describing a subgraph.
 * A Pattern can describe an entire graph, or just a single concept.
 * For example, {@code var("x").isa("movie")} is a pattern representing things that are movies.
 * A pattern can also be a conjunction: {@code and(var("x").isa("movie"), var("x").value("Titanic"))}, or a disjunction:
 * {@code or(var("x").isa("movie"), var("x").isa("tv-show"))}. These can be used to combine other patterns together
 * into larger patterns.
 */
public interface Pattern {

    Parser parser = new Parser();

    static Pattern parse(String pattern) {
        return parser.parsePatternEOF(pattern);
    }

    static List<Pattern> parseList(String pattern) {
        return parser.parsePatternListEOF(pattern).collect(Collectors.toList());
    }

    /**
     * @return a new statement with an anonymous Variable
     */
    @CheckReturnValue
    static Statement var() {
        return var(new Variable());
    }

    /**
     * @param name the name of the variable
     * @return a new statement with a variable of a given name
     */
    @CheckReturnValue
    static Statement var(String name) {
        return var(new Variable(name));
    }

    /**
     * @param var a variable to create a statement
     * @return a new statement with a provided variable
     */
    @CheckReturnValue
    static Statement var(Variable var) {
        return new Statement(var);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    static Statement label(String label) {
        return var().label(label);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    static Statement label(Label label) {
        return var().label(label);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    static Pattern and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue // TODO: replace this to accept List<Pattern>
    static Pattern and(Collection<? extends Pattern> patterns) {
        return and(new LinkedHashSet<>(patterns));
    }

    // TODO: replace this to accept LinkedHashSet<Pattern>
    static <T extends Pattern> Conjunction<T> and(Set<T> patterns) {
        return new Conjunction<>(patterns);
    }

    /**
     * @param patterns an array of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    static Pattern or(Pattern... patterns) {
        return or(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue // TODO: replace this to accept List<Pattern>
    static Pattern or(Collection<? extends Pattern> patterns) {
        // Simplify representation when there is only one alternative
        if (patterns.size() == 1) {
            return patterns.iterator().next();
        }

        return or(new LinkedHashSet<>(patterns));

//        LinkedHashSet<Conjunction<?>> conjunctions = patterns.stream().map(pattern -> {
//            Objects.requireNonNull(pattern);
//            if (pattern instanceof Conjunction<?>) return (Conjunction<?>) pattern;
//            else return new Conjunction<>(Collections.singleton(pattern));
//        }).collect(Collectors.toCollection(LinkedHashSet::new));
//
//        return or(conjunctions);
    }

    // TODO: replace this to accept LinkedHashSet<Pattern>
    static <T extends Pattern> Disjunction<T> or(Set<T> patterns) {
        return new Disjunction<>(patterns);
    }

    /**
     * Get all common, user-defined Variables in the Pattern.
     */
    @CheckReturnValue
    Set<Variable> variables();

    /**
     * @return all variables referenced in the pattern
     */
    @CheckReturnValue
    default Set<Statement> statements() {
        return getDisjunctiveNormalForm().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return this Pattern as a Statement, if it is one.
     */
    @CheckReturnValue
    default Statement asStatement() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern as a Disjunction, if it is one.
     */
    @CheckReturnValue
    default Disjunction<?> asDisjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return this Pattern as a Conjunction, if it is one.
     */
    @CheckReturnValue
    default Conjunction<?> asConjunction() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return true if this Pattern is a Statement
     */
    @CheckReturnValue
    default boolean isStatement() {
        return false;
    }

    /**
     * @return true if this Pattern is a Conjunction
     */
    @CheckReturnValue
    default boolean isDisjunction() {
        return false;
    }

    /**
     * @return true if this Pattern is a Disjunction
     */
    @CheckReturnValue
    default boolean isConjunction() {
        return false;
    }

    /**
     * Get the disjunctive normal form of this pattern group.
     * This means the pattern group will be transformed into a number of conjunctive patterns, where each is disjunct.
     *
     * e.g.
     * p = (A or B) and (C or D)
     * p.getDisjunctiveNormalForm() = (A and C) or (A and D) or (B and C) or (B and D)
     *
     * @return the pattern group in disjunctive normal form
     */
    @CheckReturnValue
    Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm();
}
