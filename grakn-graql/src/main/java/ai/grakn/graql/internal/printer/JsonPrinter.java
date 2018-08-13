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

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Concept;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.AnswerGroup;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.ConceptSetMeasure;
import ai.grakn.util.CommonUtil;
import mjson.Json;

import java.util.Collection;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Class to print Graql Responses in to JSON Formatted Strings
 *
 * @author Grakn Warriors
 */
class JsonPrinter extends Printer<Json> {
    @Override
    protected final String complete(Json builder) {
        return builder.toString();
    }

    @Override
    protected Json concept(Concept concept) {
        Json json = Json.object("id", concept.id().getValue());

        if (concept.isSchemaConcept()) {
            json.set("name", concept.asSchemaConcept().label().getValue());
            SchemaConcept superConcept = concept.asSchemaConcept().sup();
            if (superConcept != null) json.set("sub", superConcept.label().getValue());
        } else if (concept.isThing()) {
            json.set("isa", concept.asThing().type().label().getValue());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }

        if (concept.isAttribute()) {
            json.set("value", concept.asAttribute().value());
        }

        if (concept.isRule()) {
            Pattern when = concept.asRule().when();
            if (when != null) {
                json.set("when", when.toString());
            }
            Pattern then = concept.asRule().then();
            if (then != null) {
                json.set("then", then.toString());
            }
        }

        return json;
    }

    @Override
    protected final Json bool(boolean bool) {
        return Json.make(bool);
    }

    @Override
    public final Json collection(Collection<?> collection) {
        return Json.make(collection.stream().map(item -> build(item)).collect(toList()));
    }

    @Override
    protected final Json map(Map<?, ?> map) {
        Json json = Json.object();

        map.forEach((Object key, Object value) -> {
            if (key instanceof Var) key = ((Var) key).getValue();
            String keyString = key == null ? "" : key.toString();
            json.set(keyString, build(value));
        });

        return json;
    }

    @Override
    protected Json answerGroup(AnswerGroup<?> answer) {
        Json json = Json.object();
        json.set(answer.toString(), build(answer.answers()));
        return json;
    }

    @Override
    protected Json conceptMap(ConceptMap answer) {
        return map(answer.map());
    }

    @Override
    protected Json conceptSetMeasure(ConceptSetMeasure answer) {
        Json json = Json.object();
        json.set(answer.measurement().toString(), collection(answer.set()));
        return json;
    }

    @Override
    protected final Json object(Object object) {
        return Json.make(object);
    }
}
