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

package grakn.core.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.INSERT_ABSTRACT_NOT_TYPE;
import static grakn.core.common.exception.ErrorMessage.INSERT_RECURSIVE;
import static grakn.core.common.exception.ErrorMessage.INSERT_UNDEFINED_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.INVALID_VALUE;
import static grakn.core.common.exception.ErrorMessage.NEGATIVE_OFFSET;
import static grakn.core.common.exception.ErrorMessage.NON_POSITIVE_LIMIT;
import static grakn.core.common.exception.ErrorMessage.UNEXPECTED_RESULT;

/**
 * Graql Query Exception
 * Occurs when the query is syntactically correct but semantically incorrect.
 * For example limiting the results of a query -1
 */
public class GraqlQueryException extends GraknException {

    private final String NAME = "GraqlQueryException";

    private GraqlQueryException(String error) {
        super(error, null, false, false);
    }

    private GraqlQueryException(String error, Exception cause) {
        super(error, cause, false, false);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static GraqlQueryException create(String formatString, Object... args) {
        return new GraqlQueryException(String.format(formatString, args));
    }

    public static GraqlQueryException labelNotFound(Label label) {
        return new GraqlQueryException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
    }

    public static GraqlQueryException attributeWithNonAttributeType(Label attributeType) {
        return new GraqlQueryException(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage(attributeType));
    }

    public static GraqlQueryException relationWithNonRelationType(Label label) {
        return new GraqlQueryException(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(label));
    }

    public static GraqlQueryException invalidRoleLabel(Label label) {
        return new GraqlQueryException(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(label, label));
    }

    public static GraqlQueryException matchWithoutAnyProperties(Statement statement) {
        return create("Require statement to have at least one property: `%s`", statement);
    }

    public static GraqlQueryException unboundComparisonVariables(Set<Variable> unboundVariables) {
        return GraqlQueryException.create("Variables used in comparisons cannot be unbounded %s", unboundVariables.toString());
    }

    public static GraqlQueryException kCoreOnRelationType(Label label) {
        return create("cannot compute coreness of relation type %s.", label.getValue());
    }

    public static GraqlQueryException deleteSchemaConcept(SchemaConcept schemaConcept) {
        return create("cannot delete schema concept %s. Use `undefine` instead.", schemaConcept);
    }

    public static GraqlQueryException insertUnsupportedProperty(String propertyName) {
        return GraqlQueryException.create("inserting property '%s' is not supported, try `define`", propertyName);
    }

    public static GraqlQueryException defineUnsupportedProperty(String propertyName) {
        return GraqlQueryException.create("defining property '%s' is not supported, try `insert`", propertyName);
    }

    public static GraqlQueryException mustBeAttributeType(Label attributeType) {
        return new GraqlQueryException(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage(attributeType));
    }

    public static GraqlQueryException cannotGetInstancesOfNonType(Label label) {
        return GraqlQueryException.create("%s is not a type and so does not have instances", label);
    }

    public static GraqlQueryException insertPredicate() {
        return new GraqlQueryException(ErrorMessage.INSERT_PREDICATE.getMessage());
    }

    public static GraqlQueryException insertRecursive(Statement var) {
        return new GraqlQueryException(INSERT_RECURSIVE.getMessage(var.getPrintableName()));
    }

    public static GraqlQueryException insertUndefinedVariable(Statement var) {
        return new GraqlQueryException(INSERT_UNDEFINED_VARIABLE.getMessage(var.getPrintableName()));
    }

    public static GraqlQueryException createInstanceOfMetaConcept(Variable var, Type type) {
        return new GraqlQueryException(var + " cannot be an instance of meta-type " + type.label());
    }

    /**
     * Thrown when a concept is inserted with multiple properties when it can only have one.
     * <p>
     * For example: {@code insert $x isa movie; $x isa person;}
     * </p>
     */
    public static GraqlQueryException insertMultipleProperties(
            Statement varPattern, String property, Object value1, Object value2
    ) {
        String message = "a concept `%s` cannot have multiple properties `%s` and `%s` for `%s`";
        return create(message, varPattern, value1, value2, property);
    }

    /**
     * Thrown when a property is inserted on a concept that already exists and that property can't be overridden.
     * <p>
     * For example: {@code match $x isa name; insert $x val "Bob";}
     * </p>
     */
    public static GraqlQueryException insertPropertyOnExistingConcept(String property, Object value, Concept concept) {
        return create("cannot insert property `%s %s` on existing concept `%s`", property, value, concept);
    }

    /**
     * Thrown when a property is inserted on a concept that doesn't support that property.
     * <p>
     * For example, an entity with a value: {@code insert $x isa movie, val "The Godfather";}
     * </p>
     */
    public static GraqlQueryException insertUnexpectedProperty(String property, Object value, Concept concept) {
        return create("unexpected property `%s %s` for concept `%s`", property, value, concept);
    }

    /**
     * Thrown when a concept does not have all expected properties required to insert it.
     * <p>
     * For example, an attribute without a value: {@code insert $x isa name;}
     * </p>
     */
    public static GraqlQueryException insertNoExpectedProperty(String property, Statement var) {
        return create("missing expected property `%s` in `%s`", property, var);
    }



    /**
     * Thrown when attempting to insert a concept that already exists.
     * <p>
     * For example: {@code match $x isa movie; insert $x isa name, val "Bob";}
     * </p>
     */
    public static GraqlQueryException insertExistingConcept(Statement pattern, Concept concept) {
        return create("cannot overwrite properties `%s` on  concept `%s`", pattern, concept);
    }

    public static GraqlQueryException nonPositiveLimit(long limit) {
        return new GraqlQueryException(NON_POSITIVE_LIMIT.getMessage(limit));
    }

    public static GraqlQueryException negativeOffset(long offset) {
        return new GraqlQueryException(NEGATIVE_OFFSET.getMessage(offset));
    }

    public static GraqlQueryException invalidValueClass(Object value) {
        return new GraqlQueryException(INVALID_VALUE.getMessage(value.getClass()));
    }

    public static GraqlQueryException unknownAggregate(String name) {
        return new GraqlQueryException(ErrorMessage.UNKNOWN_AGGREGATE.getMessage(name));
    }

    public static GraqlQueryException maxIterationsReached(Class<?> clazz) {
        return new GraqlQueryException(ErrorMessage.MAX_ITERATION_REACHED.getMessage(clazz.toString()));
    }

    public static GraqlQueryException statisticsAttributeTypesNotSpecified() {
        return new GraqlQueryException(ErrorMessage.ATTRIBUTE_TYPE_NOT_SPECIFIED.getMessage());
    }

    public static GraqlQueryException instanceDoesNotExist() {
        return new GraqlQueryException(ErrorMessage.INSTANCE_DOES_NOT_EXIST.getMessage());
    }

    public static GraqlQueryException kValueSmallerThanTwo() {
        return new GraqlQueryException(ErrorMessage.K_SMALLER_THAN_TWO.getMessage());
    }

    public static GraqlQueryException attributeMustBeANumber(AttributeType.DataType dataType, Label attributeType) {
        return new GraqlQueryException(attributeType + " must have data type of `long` or `double`, but was " + dataType.name());
    }

    public static GraqlQueryException attributesWithDifferentDataTypes(Collection<String> attributeTypes) {
        return new GraqlQueryException("resource types " + attributeTypes + " have different data types");
    }

    public static GraqlQueryException unificationAtomIncompatibility() {
        return new GraqlQueryException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
    }

    public static GraqlQueryException nonGroundNeqPredicate(ReasonerQuery query) {
        return new GraqlQueryException(ErrorMessage.NON_GROUND_NEQ_PREDICATE.getMessage(query));
    }

    public static GraqlQueryException incompleteResolutionPlan(ReasonerQuery reasonerQuery) {
        return new GraqlQueryException(ErrorMessage.INCOMPLETE_RESOLUTION_PLAN.getMessage(reasonerQuery));
    }

    public static GraqlQueryException rolePatternAbsent(Atomic relation) {
        return new GraqlQueryException(ErrorMessage.ROLE_PATTERN_ABSENT.getMessage(relation));
    }

    public static GraqlQueryException nonExistentUnifier() {
        return new GraqlQueryException(ErrorMessage.NON_EXISTENT_UNIFIER.getMessage());
    }

    public static GraqlQueryException illegalAtomConversion(Atomic atom, Class<?> targetType) {
        return new GraqlQueryException(ErrorMessage.ILLEGAL_ATOM_CONVERSION.getMessage(atom, targetType));
    }

    public static GraqlQueryException valuePredicateAtomWithMultiplePredicates() {
        return new GraqlQueryException("Attempting creation of ValuePredicate atom with more than single predicate");
    }

    public static GraqlQueryException getUnifierOfNonAtomicQuery() {
        return new GraqlQueryException("Attempted to obtain unifiers on non-atomic queries.");
    }

    public static GraqlQueryException unsafeNegationBlock(ResolvableQuery query) {
        return new GraqlQueryException(ErrorMessage.UNSAFE_NEGATION_BLOCK.getMessage(query));
    }

    public static GraqlQueryException usingNegationWithReasoningOff(Pattern pattern) {
        return new GraqlQueryException(ErrorMessage.USING_NEGATION_WITH_REASONING_OFF.getMessage(pattern));
    }

    public static GraqlQueryException disjunctiveNegationBlock() {
        return new GraqlQueryException(ErrorMessage.DISJUNCTIVE_NEGATION_BLOCK.getMessage());
    }

    public static GraqlQueryException invalidQueryCacheEntry(ReasonerQuery query, ConceptMap answer) {
        return new GraqlQueryException(ErrorMessage.INVALID_CACHE_ENTRY.getMessage(query.toString(), answer.toString()));
    }

    public static GraqlQueryException conceptNotAThing(Object value) {
        return new GraqlQueryException(ErrorMessage.CONCEPT_NOT_THING.getMessage(value));
    }

    public static GraqlQueryException nonRoleIdAssignedToRoleVariable(Statement var) {
        return new GraqlQueryException(ErrorMessage.ROLE_ID_IS_NOT_ROLE.getMessage(var.toString()));
    }

    public static GraqlQueryException cannotParseDateFormat(String originalFormat) {
        return new GraqlQueryException("Cannot parse date format " + originalFormat + ". See DateTimeFormatter#ofPattern");
    }

    public static GraqlQueryException cannotParseDateString(String originalDate, String originalFormat, DateTimeParseException cause) {
        throw new GraqlQueryException("Cannot parse date value " + originalDate + " with format " + originalFormat, cause);
    }

    public static GraqlQueryException noLabelSpecifiedForHas(Variable var) {
        return create("'has' argument '%s' requires a label", var);
    }

    public static GraqlQueryException insertRolePlayerWithoutRoleType() {
        return new GraqlQueryException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage());
    }

    public static GraqlQueryException insertAbstractOnNonType(SchemaConcept concept) {
        return new GraqlQueryException(INSERT_ABSTRACT_NOT_TYPE.getMessage(concept.label()));
    }

    public static GraqlQueryException unexpectedResult(Variable var) {
        return new GraqlQueryException(UNEXPECTED_RESULT.getMessage(var.name()));
    }

}
