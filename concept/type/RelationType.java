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

package grakn.core.concept.type;

import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.concept.thing.Relation;

public interface RelationType extends ThingType {

    @Override
    FunctionalIterator<? extends RelationType> getSubtypes();

    @Override
    FunctionalIterator<? extends RelationType> getSubtypesExplicit();

    @Override
    FunctionalIterator<? extends Relation> getInstances();

    void setSupertype(RelationType superType);

    void setRelates(String roleLabel);

    void setRelates(String roleLabel, String overriddenLabel);

    void unsetRelates(String roleLabel);

    FunctionalIterator<? extends RoleType> getRelates();

    FunctionalIterator<? extends RoleType> getRelatesExplicit();

    RoleType getRelates(String roleLabel);

    RoleType getRelatesExplicit(String roleLabel);

    RoleType getRelatesOverridden(String roleLabel);

    Relation create();

    Relation create(boolean isInferred);
}
