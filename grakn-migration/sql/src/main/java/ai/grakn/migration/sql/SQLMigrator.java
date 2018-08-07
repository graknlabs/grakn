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

package ai.grakn.migration.sql;

import ai.grakn.migration.base.MigrationCLI;
import com.google.common.collect.Maps;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The SQL migrator will execute the given SQL query and then apply the given template to those results.
 * @author alexandraorth
 */
public class SQLMigrator {

    private final Stream<Record> records;

    public static void main(String[] args) {
        try {
            MigrationCLI.init(args, SQLMigrationOptions::new).stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(SQLMigrator::runSQL);
        } catch (IllegalArgumentException e){
            System.err.println(e.getMessage());
        }
    }

    private static void runSQL(SQLMigrationOptions options) {
        try {
            File sqlTemplate = new File(options.getTemplate());

            if(!sqlTemplate.exists()){
                System.err.println("Cannot find file: " + options.getTemplate());
                return;
            }

            if(options.hasDriver()) {
                DriverManager.registerDriver(options.getDriver());
            }

            try(Connection connection = DriverManager.getConnection(options.getLocation(),
                    options.getUsername(), options.getPassword())) {

                SQLMigrator sqlMigrator = new SQLMigrator(options.getQuery(), connection);

                MigrationCLI.loadOrPrint(sqlTemplate, sqlMigrator.convert(), options);
            }
        } catch (Throwable throwable){
            System.err.println(throwable.getMessage());
        }
    }

    /**
     * Construct a SQL migrator to migrate data from the given DB
     * @param query SQL query to gather data from database
     * @param connection JDBC connection to the SQL database
     */
    public SQLMigrator(String query, Connection connection){
        DSLContext create = DSL.using(connection);
        records = create.fetchStream(query);
    }

    /**
     * Migrate the results of the SQL statement with the provided template
     * @return stream of parsed insert queries
     */
    public Stream<Map<String, Object>> convert() {
        return records.map(Record::intoMap).map(this::convertToValidValues);
    }

    /**
     * Convert values to be valid - removing nulls and changing to supported types
     * @param data data to make valid
     * @return valid data
     */
    private Map<String, Object> convertToValidValues(Map<String, Object> data){
        data = Maps.filterValues(data, Objects::nonNull);
        data = Maps.transformValues(data, this::convertToSupportedTypes);
        return data;
    }

    /**
     * If a SQL value is not one of the supported types, convert it to a string
     * @param object object to convert
     * @return object as one of the supported types
     */
    private Object convertToSupportedTypes(Object object) {
        if(!(object instanceof String ||
            object instanceof Number ||
            object instanceof List ||
            object instanceof Boolean)){
            return object.toString();
        }

        return object;
    }
}
