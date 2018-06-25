/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package storage;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * Stores identifiers for all concepts in a Grakn
 */
public class IgniteConceptIdStore implements ConceptStore {
    private Connection conn;
    private Set<String> typeLabels;
    String cachingMethod = "REPLICATED";

    public IgniteConceptIdStore(Set<String> typeLabels) throws ClassNotFoundException, SQLException {
        this.typeLabels = typeLabels;

        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Open JDBC connection.
        this.conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

        // Create database tables.
        for (String typeLabel : this.typeLabels) {
            this.createConceptIdTable(typeLabel);
        }

    }

    private void createConceptIdTable(String typeLabel) throws SQLException {
        createTable(typeLabel, "VARCHAR");
    }

    private void createTable(String typeLabel, String sqlDatatypeName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + typeLabel + " (" +
                    " id " + sqlDatatypeName + " PRIMARY KEY, " +
                    "nothing LONG) " +
                    " WITH \"template=" + cachingMethod + "\"");
        }
    }

    @Override
    public void add(Concept concept) {

        Label conceptTypeLabel = concept.asThing().type().getLabel();
        String conceptId = concept.asThing().getId().toString(); // TODO use the value instead for attributes

        try (PreparedStatement stmt = this.conn.prepareStatement(
                "INSERT INTO " + conceptTypeLabel + " (id, ) VALUES (?, )")) {

            stmt.setString(1, conceptId);
            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
    [{ LIMIT expression [OFFSET expression]
    [SAMPLE_SIZE rowCountInt]} | {[OFFSET expression {ROW | ROWS}]
    [{FETCH {FIRST | NEXT} expression {ROW | ROWS} ONLY}]}]
     */

    public String get(String typeLabel, int offset) {

        String sql = "SELECT id FROM " + typeLabel +
                " OFFSET " + offset +
                " FETCH FIRST ROW ONLY";
//        ResultSet rs = this.runQuery(sql);

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {

                if (rs != null && rs.next()) { // Need to do this to increment one line in the ResultSet
                    return rs.getString(1);
                } else {
                    return null;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int typeCount(String typeLabel) {

        String sql = "SELECT COUNT(1) FROM " + typeLabel;
//        ResultSet rs = this.runQuery(sql);

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {

                if (rs != null && rs.next()) { // Need to do this to increment one line in the ResultSet
                    return rs.getInt(1);
                } else {
                    return 0;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private ResultSet runQuery(String sql) {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return rs;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Clean all elements in the storage
     */
    public static void clean(Set<String> typeLabels) throws SQLException {
        for (String typeLabel : typeLabels) {
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
            try (PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS " + typeLabel)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
