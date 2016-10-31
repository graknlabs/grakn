/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.graql.internal.template.macro;

import io.grakn.graql.internal.template.Value;

import java.util.List;

public class EqualsMacro implements Macro<Boolean> {

    @Override
    public Boolean apply(List<Value> values) {
        if(values.size() < 2){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        Value first = values.get(0);
        return values.stream().allMatch(first::equals);
    }

    @Override
    public String name() {
        return "equals";
    }
}
