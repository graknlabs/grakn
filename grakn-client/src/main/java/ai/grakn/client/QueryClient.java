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
package ai.grakn.client;

import static org.apache.http.HttpHost.DEFAULT_SCHEME_NAME;
import static org.apache.http.HttpStatus.SC_OK;

import java.net.URI;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import static ai.grakn.util.REST.WebPath.Graph.GRAQL;
import mjson.Json;

/**
 * <p>
 * A client for sending Graql queries to the GRAKN.AI Engine. Queries are formulated
 * as Graql strings and the format of the response is configured as JSON, JSON+HAL or
 * plain text.
 * </p>
 * 
 * @author borislav
 *
 */
public class QueryClient extends Client {
    
    private final HttpClient httpClient = HttpClients.createDefault();
    private String scheme = DEFAULT_SCHEME_NAME;
    private String host;
    private int port;
    private String keyspace = "grakn";
    private boolean infer = true;
    private boolean materialise = false;
    
    /**
     * Default constructor - do not rely on the default values, plus use fluid setter methods to
     * initialize the host name and port. The default keyspace is <code>grakn</code> and the 
     * default protocol is <code>http</code>.
     */
    public QueryClient() {        
    }

    /**
     * Construct a query client against the given host and port number.
     * 
     * @param host Just the host name, with the schema or port etc.
     * @param port The port number to use.
     */
    public QueryClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String scheme() { 
        return scheme; 
    }
    
    public QueryClient scheme(String scheme) { 
        this.scheme = scheme; 
        return this; 
    }
    
    public String host() { 
        return host; 
    }
    
    public QueryClient host(String host) { 
        this.host = host; 
        return this; 
    }

    public String keyspace() { 
        return keyspace; 
    }
    
    public QueryClient keyspace(String keyspace) { 
        this.keyspace = keyspace; 
        return this; 
    }
    
    public int port() { 
        return port; 
    }
    
    public QueryClient port(int port) { 
        this.port = port; 
        return this; 
    }

    public Json query(String query) {
        return query(keyspace, query, infer, materialise);
    }
    
    /**
     * <p>
     * Send a query to the server against the specifying keyspace. 
     * </p>
     * 
     * @param keyspace The keyspace (database name) holding the knowledge graph.
     * @param query A valid Graqlq query.
     * @param infer Whether to use inference while performing the query.
     * @param materialise Whether to save all inferences method during the query process.
     * @return The JSON response as specifying by the GRAKN Engine REST API.
     */
    public Json query(String keyspace, String query, boolean infer, boolean materialise) {
        try {
            URI uri = new URIBuilder(GRAQL)
                    .setScheme(DEFAULT_SCHEME_NAME)
                    .setPort(port)
                    .setHost(host)
                    .addParameter("keyspace", keyspace)
                    .addParameter("query", query)
                    .addParameter("infer", Boolean.toString(infer))
                    .addParameter("materialise", Boolean.toString(materialise))
                    .build();
            HttpGet httpGet = new HttpGet(uri);
            httpGet.addHeader("Accept", "application/graql+json");
            HttpResponse response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() != SC_OK) {
                throw new Exception("Server returned status: " + response.getStatusLine().getStatusCode() + 
                                ", entity=" + asStringHandler.handleResponse(response));
            }
            return asJsonHandler.handleResponse(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    // Main for testing purposes, not intended for public use. 
    public static void main(String []argv) {
        QueryClient client = new QueryClient("localhost", 4567).keyspace("snb");
        System.out.println(client.query("match $x isa person; offset 0; limit 30;"));
    }
}
