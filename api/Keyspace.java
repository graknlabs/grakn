/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.api;

import java.util.regex.Pattern;

public interface Keyspace extends Comparable<Keyspace > {

    int MAX_LENGTH = 48;

    String name();

    static boolean isValidName(String name) {
        return Pattern.matches("[a-z_][a-z_0-9]*", name) && name.length() <= MAX_LENGTH;
    }

    @Override
    default int compareTo(Keyspace o) {
        if (equals(o)) return 0;
        return name().compareTo(o.name());
    }
}
