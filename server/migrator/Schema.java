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
 *
 */

package grakn.core.server.migrator;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.ThingType;
import grakn.core.logic.Rule;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static graql.lang.common.util.Strings.escapeRegex;
import static graql.lang.common.util.Strings.quoteString;

public class Schema {

    private static String SEMICOLON_NEWLINE_X2 = ";\n\n";
    private static String COMMA_NEWLINE_INDENT = ",\n" + indent(1);
    private final Grakn grakn;
    private final String database;

    public Schema(final Grakn grakn, final String database) {
        this.grakn = grakn;
        this.database = database;
    }

    public String getSchema() {
        StringBuilder builder = new StringBuilder();
        builder.append("define\n\n");
        try (final Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA);
             final Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
            tx.concepts()
                    .getRootAttributeType()
                    .getSubtypesDirect()
                    .sorted(Comparator.comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeAttributeType(builder, x));
            tx.concepts()
                    .getRootRelationType()
                    .getSubtypesDirect()
                    .sorted(Comparator.comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeRelationType(builder, x));
            tx.concepts()
                    .getRootEntityType()
                    .getSubtypesDirect()
                    .sorted(Comparator.comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeEntityType(builder, x));
            tx.logic().rules()
                    .stream()
                    .sorted(Comparator.comparing(x -> x.getLabel()))
                    .forEach(x -> writeRule(builder, x));
        }
        return builder.toString();
    }

    private void writeAttributeType(StringBuilder builder, AttributeType attributeType) {
        builder.append(String.format("%s sub %s",
                attributeType.getLabel().name(),
                attributeType.getSupertype().getLabel().name()))
                .append(COMMA_NEWLINE_INDENT)
                .append(String.format("value %s", getValueTypeString(attributeType.getValueType())));
        if (attributeType instanceof AttributeType.String) {
            java.util.regex.Pattern regex = attributeType.asString().getRegex();
            if (regex != null) {
                builder.append(COMMA_NEWLINE_INDENT)
                        .append(String.format("regex %s", quoteString(escapeRegex(regex.pattern()))));
            }
        }
        writeAbstract(builder, attributeType);
        writeOwns(builder, attributeType);
        writePlays(builder, attributeType);
        builder.append(SEMICOLON_NEWLINE_X2);
        attributeType.getSubtypesDirect()
                .sorted(Comparator.comparing(x -> x.getLabel().name()))
                .forEach(x -> writeAttributeType(builder, x));
    }

    private void writeRelationType(StringBuilder builder, RelationType relationType) {
        builder.append(String.format("%s sub %s",
                relationType.getLabel().name(),
                relationType.getSupertype().getLabel().name()));
        writeAbstract(builder, relationType);
        writeOwns(builder, relationType);
        writeRelates(builder, relationType);
        writePlays(builder, relationType);
        builder.append(SEMICOLON_NEWLINE_X2);
        relationType.getSubtypesDirect()
                .sorted(Comparator.comparing(x -> x.getLabel().name()))
                .forEach(x -> writeRelationType(builder, x));
    }

    private void writeEntityType(StringBuilder builder, EntityType entityType) {
        builder.append(String.format("%s sub %s",
                entityType.getLabel().name(),
                entityType.getSupertype().getLabel().name()));
        writeAbstract(builder, entityType);
        writeOwns(builder, entityType);
        writePlays(builder, entityType);
        builder.append(SEMICOLON_NEWLINE_X2);
        entityType.getSubtypesDirect()
                .sorted(Comparator.comparing(x -> x.getLabel().name()))
                .forEach(x -> writeEntityType(builder, x));
    }

    private void writeAbstract(StringBuilder builder, ThingType thingType) {
        if (thingType.isAbstract()) {
            builder.append(COMMA_NEWLINE_INDENT).append("abstract");
        }
    }

    private void writeOwns(StringBuilder builder, ThingType thingType) {
        Set<String> keys = thingType.getOwnsDirect(true).map(x -> x.getLabel().name()).collect(Collectors.toSet());
        List<String> attributeTypes = thingType.getOwnsDirect().map(x -> x.getLabel().name()).collect(Collectors.toList());
        attributeTypes.stream().filter(x -> keys.contains(x)).sorted()
                .forEach(x -> builder
                        .append(COMMA_NEWLINE_INDENT)
                        .append(String.format("owns %s @key", x)));
        attributeTypes.stream().filter(type -> !keys.contains(type)).sorted()
                .forEach(x -> builder
                        .append(COMMA_NEWLINE_INDENT)
                        .append(String.format("owns %s", x)));
    }

    private void writeRelates(StringBuilder builder, RelationType relationType) {
        relationType.getRelatesDirect().sorted(Comparator.comparing(x -> x.getLabel().name()))
                .forEach(x -> builder
                        .append(COMMA_NEWLINE_INDENT)
                        .append(String.format("relates %s", x.getLabel().name())));
    }

    private void writePlays(StringBuilder builder, ThingType thingType) {
        thingType.getPlaysDirect().sorted(Comparator.comparing(x -> x.getLabel().scopedName()))
                .forEach(x -> builder
                        .append(COMMA_NEWLINE_INDENT)
                        .append(String.format("plays %s", x.getLabel().scopedName())));
    }

    private void writeRule(StringBuilder builder, Rule rule) {
        builder.append(String.format("rule %s:\n", rule.getLabel()))
                .append(indent(1))
                .append("when\n")
                .append(getPatternString(wrapConjunction(rule.getWhenPreNormalised()), 1))
                .append("\n")
                .append(indent(1))
                .append("then\n")
                .append(getPatternString(wrapConjunction(rule.getThenPreNormalised()), 1))
                .append(SEMICOLON_NEWLINE_X2);
    }

    private String getValueTypeString(AttributeType.ValueType valueType) {
        switch (valueType) {
            case STRING:
                return "string";
            case LONG:
                return "long";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case DATETIME:
                return "datetime";
            default:
                throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private String getPatternString(Pattern pattern, int indent) {
        if (pattern.isVariable()) {
            return indent(indent) + pattern.asVariable().toString();
        } else if (pattern.isConjunction()) {
            StringBuilder builder = new StringBuilder()
                    .append(indent(indent))
                    .append("{\n");
            pattern.asConjunction().patterns().forEach(p -> builder
                    .append(getPatternString(p, indent + 1))
                    .append(";\n"));
            builder.append(indent(indent))
                    .append("}");
            return builder.toString();
        } else if (pattern.isDisjunction()) {
            return pattern.asDisjunction().patterns().stream()
                    .map(p -> getPatternString(wrapConjunction(p), indent))
                    .collect(Collectors.joining("\n" + indent(indent) + "or\n"));
        } else if (pattern.isNegation()) {
            return indent(indent) + "not\n" + getPatternString(wrapConjunction(pattern.asNegation().pattern()), indent);
        } else {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private Pattern wrapConjunction(Pattern pattern) {
        return pattern.isConjunction() ? pattern : new Conjunction<>(list(pattern));
    }

    private static String indent(int indent) {
        return IntStream.range(0, indent * 4).mapToObj(i -> " ").collect(Collectors.joining());
    }
}