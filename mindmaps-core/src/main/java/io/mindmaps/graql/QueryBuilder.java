package io.mindmaps.graql;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public interface QueryBuilder {

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    MatchQuery match(Pattern... patterns);

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    MatchQuery match(Collection<? extends Pattern> patterns);

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    InsertQuery insert(Var... vars);

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    InsertQuery insert(Collection<? extends Var> vars);

    ComputeQuery compute(String computeMethod);

    ComputeQuery compute(String computeMethod, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds);

    /**
     * @param inputStream a stream representing a list of patterns
     * @return a stream of patterns
     */
    Stream<Pattern> parsePatterns(InputStream inputStream);

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    List<Pattern> parsePatterns(String patternsString);

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    <T extends Query<?>> T parse(String queryString);

    /**
     * @param template a string representing a templated graql query
     * @param data data to use in template
     * @return a resolved graql query
     */
    String parseTemplate(String template, Map<String, Object> data);

    void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod);
}
