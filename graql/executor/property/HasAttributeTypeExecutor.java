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

package grakn.core.graql.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.sets.HasFragmentSet;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static graql.lang.Graql.var;

public class HasAttributeTypeExecutor  implements PropertyExecutor.Definable {

    private final Variable var;
    private final HasAttributeTypeProperty property;

    HasAttributeTypeExecutor(Variable var, HasAttributeTypeProperty property) {
        this.var = var;
        this.property = property;

//        // TODO: this may the cause of issue #4664
//        String type = attributeType.getType().orElseThrow(
//                () -> GraqlSemanticException.noLabelSpecifiedForHas(attributeType.var())
//        );
//
//        Statement role = Graql.type(Graql.Token.Type.ROLE);
//
//         TODO: To fix issue #4664 (querying schema with variable attribute type), it's not enough to just remove the
//               exception handling above. We need to be able to restrict the direcitonality of the following ownerRole
//               and valueRole. For example, for keys, we restrict the role names to start with `key-`. However, we
//               cannot do this because the name of the variable attribute type is unknown. This means that we need to
//               change the data structure so that we have meta-super-roles for attribute-owners and attribute-values
//        Statement ownerRole = var().sub(role);
//        Statement valueRole = var().sub(role);
//        Statement relationType = var().sub(Graql.type(Graql.Token.Type.RELATION));
//
        // If a key, limit only to the implicit key type
//        if (property.isKey()) {
//            ownerRole = ownerRole.type(KEY_OWNER.getLabel(type).getValue());
//            valueRole = valueRole.type(KEY_VALUE.getLabel(type).getValue());
//            relationType = relationType.type(KEY.getLabel(type).getValue());
//        }

//        Statement relationOwner = relationType.relates(ownerRole);
//        Statement relationValue = relationType.relates(valueRole);
//
//        this.ownerRole = ownerRole;
//        this.valueRole = valueRole;
//        if (relationOwner == null) {
//            throw new NullPointerException("Null relationOwner");
//        }
//        this.relationOwner = relationOwner;
//        if (relationValue == null) {
//            throw new NullPointerException("Null relationValue");
//        }
//        this.relationValue = relationValue;

    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(new HasFragmentSet(property, var, property.attributeType().var()));
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineHasAttributeType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineHasAttributeType());
    }

    private abstract class HasAttributeTypeWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.attributeType().var());
            return Collections.unmodifiableSet(required);
        }

        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }
    }

    private class DefineHasAttributeType extends HasAttributeTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Type typeConcept = executor.getConcept(var).asType();
            AttributeType attributeTypeConcept = executor
                    .getConcept(property.attributeType().var())
                    .asAttributeType();
            if (property.isKey()) {
                typeConcept.key(attributeTypeConcept);
            } else {
                typeConcept.has(attributeTypeConcept);
            }
        }
    }

    private class UndefineHasAttributeType extends HasAttributeTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Type type = executor.getConcept(var).asType();
            AttributeType<?> attributeType = executor
                    .getConcept(property.attributeType().var())
                    .asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (property.isKey()) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        }
    }
}
