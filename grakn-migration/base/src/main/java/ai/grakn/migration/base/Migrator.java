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

package ai.grakn.migration.base;

import ai.grakn.Keyspace;
import ai.grakn.batch.BatchExecutorClient;
import ai.grakn.batch.GraknClient;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryParser;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>
 * Abstract migrator class containing methods and functionality needed by
 * all extending migrator classes.
 * </p>
 *
 * @author alexandraorth
 * @author Domenico Corapi
 */
public class Migrator {

    private final static Logger LOG = LoggerFactory.getLogger(Migrator.class);

    private final QueryParser queryParser = Graql.withoutGraph().infer(false).parser();
    private final SimpleURI uri;
    private final Keyspace keyspace;
    private final int retries;
    private final boolean failFast;
    private final int maxDelayMs;
    private final int maxLines;
    private final MetricRegistry metricRegistry;
    private final ConsoleReporter reporter;
    private final Meter totalMeter;
    private final Meter successMeter;
    private final Timer parseTemplate;

    /**
     * @param uri Uri where one instance of Grakn Engine is running
     * @param keyspace The {@link Keyspace} where the data should be persisted
     */
    public Migrator(SimpleURI uri, Keyspace keyspace, int retries, boolean failFast, int maxDelayMs, int maxLines) {
        this.uri = uri;
        this.keyspace = keyspace;
        this.retries = retries;
        this.failFast = failFast;
        this.maxDelayMs = maxDelayMs;
        this.maxLines = maxLines;
        this.metricRegistry = new MetricRegistry();
        this.totalMeter = metricRegistry.meter(name(this.getClass(), "total"));
        this.successMeter = metricRegistry.meter(name(this.getClass(), "success"));
        this.parseTemplate = metricRegistry.timer(name(this.getClass(), "parse", "template"));
        this.reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(MILLISECONDS)
                .build();
    }

    /**
     * Print data passed in data parameter using the given template
     *
     * @param template Used to transform the data
     * @param data Data being migrated
     */
    public void print(String template, Stream<Map<String, Object>> data) {
        data.flatMap(d -> template(template, d, false)).forEach(System.out::println);
    }

    /**
     * Migrate data constrained by this migrator using a loader configured
     * by the provided parameters.
     *
     * @param template Template used to extract the data
     * @param data Data being migrated
     */
    public void load(String template, Stream<Map<String, Object>> data) {
        GraknClient graknClient = GraknClient.of(uri);

        AtomicInteger queriesExecuted = new AtomicInteger(0);

        try (BatchExecutorClient loader =
                BatchExecutorClient.newBuilder()
                        .taskClient(graknClient)
                        .maxRetries(retries)
                        .maxDelay(maxDelayMs)
                        .metricRegistry(metricRegistry)
                        .build()) {

            subscribeToReportOutcome(failFast, loader, queriesExecuted);

            Stream<Query> queryStream = data.flatMap(d -> template(template, d, failFast));
            if (maxLines > -1) {
                queryStream = queryStream.limit(maxLines);
            }
            queryStream
                    .forEach(q -> {
                        LOG.trace("Adding query {}", q);
                        totalMeter.mark();
                        loader.add(q, keyspace);
                    });
        }

        System.out.println("Loaded " + queriesExecuted + " statements");
    }

    private void subscribeToReportOutcome(
            boolean failFast, BatchExecutorClient batchExecutorClient, AtomicInteger queriesExecuted
    ) {
        batchExecutorClient.onNext(taskResult -> {
            LOG.trace("Successfully executed: {}", taskResult);
            queriesExecuted.incrementAndGet();
            successMeter.mark();
        });

        batchExecutorClient.onError(error -> {
            System.err.println("Error in execution: " + error);
            if (failFast) {
                throw GraknBackendException
                        .migrationFailure(error.getMessage());
            }
        });
    }

    /**
     * @param template a string representing a templated graql query
     * @param data data used in the template
     * @return an insert query
     */
    protected Stream<Query> template(String template, Map<String, Object> data, boolean failFast) {
        try (Context c = parseTemplate.time()){
            return queryParser.parseTemplate(template, data);
        } catch (Exception e) {
            System.out.println("Query not sent to server: " + e.getMessage());
            if (failFast) {
                throw e;
            }
        }
        return Stream.empty();
    }

    ConsoleReporter getReporter() {
        return reporter;
    }
}
