/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.migration.sql;

import ai.grakn.migration.base.AbstractMigrator;
import ai.grakn.migration.base.io.MigrationOptions;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static java.lang.Integer.parseInt;

/**
 * Configure the default SQL migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class SQLMigrationOptions extends MigrationOptions {

    private final String batch = Integer.toString(AbstractMigrator.BATCH_SIZE);

    public SQLMigrationOptions(String[] args){
        super(args);

        options.addOption("driver", true, "JDBC driver");
        options.addOption("location", true, "JDBC url (location of DB)");
        options.addOption("user", true, "JDBC username");
        options.addOption("pass", true, "JDBC password");
        options.addOption("q", "query", true, "SQL Query");
        options.addOption("t", "template", true, "template for the given SQL query");
        options.addOption("b", "batch", true, "number of row to load at once");

        parse(args);
    }

    public String getDriver() {
        if(command.hasOption("driver")){
            return command.getOptionValue("driver");
        }
        return die("No driver specified (-driver)");
    }

    public String getLocation() {
        if(command.hasOption("location")){
            return command.getOptionValue("location");
        }
        return die("No db specified (-location)");
    }

    public String getUsername() {
        if(command.hasOption("user")){
            return command.getOptionValue("user");
        }
        return die("No username specified (-user)");
    }

    public String getPassword() {
        if(command.hasOption("pass")){
            return command.getOptionValue("pass");
        }
        return die("No password specified (-pass)");
    }

    public String getQuery() {
        if(command.hasOption("query")){
            return command.getOptionValue("query");
        }
        return die("No SQL query specified (-query)");
    }

    public String getTemplate() {
        if(command.hasOption("t")){
            return command.getOptionValue("t");
        }
        return die("Template file missing (-t)");
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }
}
