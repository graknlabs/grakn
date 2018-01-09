/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.migration.xml;

import ai.grakn.migration.base.MigrationOptions;

/**
 * Configure the default XML migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class XmlMigrationOptions extends MigrationOptions {

    public XmlMigrationOptions(String[] args) {
        super();

        options.addOption("i", "input", true, "Input XML data file or directory.");
        options.addOption("s", "schema", true, "The XML Schema file name, usually .xsd extension defining with type information about the data.");        
        options.addOption("e", "element", true, "The name of the XML element to migrate - all others will be ignored.");
        options.addOption("t", "template", true, "Graql template to apply to the data.");

        parse(args);
    }

    public String getElement() {
        return command.getOptionValue("e", null);
    }

    public String getSchemaFile() {
        return command.getOptionValue("s", null);
    }
}
