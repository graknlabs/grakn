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

package ai.grakn.graql.internal.parser;

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.antlr.GraqlLexer;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.antlr.v4.runtime.UnbufferedTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Class for parsing query strings into valid queries
 *
 * @author Felix Chapman
 */
public class QueryParser {

    private final QueryBuilder queryBuilder;
    private final Map<String, Function<List<Object>, Aggregate>> aggregateMethods = new HashMap<>();

    public static final ImmutableBiMap<String, AttributeType.DataType> DATA_TYPES = ImmutableBiMap.of(
            "long", AttributeType.DataType.LONG,
            "double", AttributeType.DataType.DOUBLE,
            "string", AttributeType.DataType.STRING,
            "boolean", AttributeType.DataType.BOOLEAN,
            "date", AttributeType.DataType.DATE
    );

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     */
    private QueryParser(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Create a query parser with the specified graph
     *  @param queryBuilder the QueryBuilderImpl to operate the query on
     *  @return a query parser that operates with the specified graph
     */
    public static QueryParser create(QueryBuilder queryBuilder) {
        QueryParser parser = new QueryParser(queryBuilder);
        parser.registerDefaultAggregates();
        return parser;
    }

    private void registerAggregate(String name, int numArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        registerAggregate(name, numArgs, numArgs, aggregateMethod);
    }

    private void registerAggregate(
            String name, int minArgs, int maxArgs, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, args -> {
            if (args.size() < minArgs || args.size() > maxArgs) {
                throw GraqlQueryException.incorrectAggregateArgumentNumber(name, minArgs, maxArgs, args);
            }
            return aggregateMethod.apply(args);
        });
    }

    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        aggregateMethods.put(name, aggregateMethod);
    }

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    @SuppressWarnings("unchecked")
    public <T extends Query<?>> T parseQuery(String queryString) {
        // We can't be sure the returned query type is correct - even at runtime(!) because Java erases generics.
        //
        // e.g.
        // >> AggregateQuery<Boolean> q = qp.parseQuery("match $x isa movie; aggregate count;");
        // The above will work at compile time AND runtime - it will only fail when the query is executed:
        // >> Boolean bool = q.execute();
        // java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Boolean
        return (T) parseQueryFragment(GraqlParser::queryEOF, QueryVisitor::visitQueryEOF, queryString, getLexer(queryString));
    }

    /**
     * @param reader a reader representing several queries
     * @return a list of queries
     */
    public <T extends Query<?>> Stream<T> parseList(Reader reader) {
        UnbufferedCharStream charStream = new UnbufferedCharStream(reader);
        GraqlLexer lexer = new GraqlLexer(charStream);

        /*
            We tell the lexer to copy the text into each generated token.
            Normally when calling `Token#getText`, it will look into the underlying `TokenStream` and call
            `TokenStream#size` to check it is in-bounds. However, `UnbufferedTokenStream#size` is not supported
            (because then it would have to read the entire input). To avoid this issue, we set this flag which will
            copy over the text into each `Token`, s.t. that `Token#getText` will just look up the copied text field.
        */
        lexer.setTokenFactory(new CommonTokenFactory(true));

        return parseList(lexer, ""); // TODO
    }

    /**
     * @param queryString a string representing several queries
     * @return a list of queries
     */
    public <T extends Query<?>> Stream<T> parseList(String queryString) {
        GraqlLexer lexer = getLexer(queryString);
        return parseList(lexer, queryString);
    }

    private <T extends Query<?>> Stream<T> parseList(GraqlLexer lexer, String queryString) {
        GraqlErrorListener errorListener = new GraqlErrorListener(queryString);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        // Use an unbuffered token stream so we can handle extremely large input strings
        UnbufferedTokenStream tokenStream = new UnbufferedTokenStream(ChannelTokenSource.of(lexer));

        GraqlParser parser = new GraqlParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        parser.setErrorHandler(new BailErrorStrategy());

        Iterable<T> queryIterator = () -> new AbstractIterator<T>() {

            @Nullable
            @Override
            protected T computeNext() {
                int latestToken = tokenStream.LA(1);
                if (latestToken == Token.EOF) {
                    endOfData();
                    return null;
                } else {
                    Query<?> query = parseQueryFragment(GraqlParser::query, QueryVisitor::visitQuery, parser, errorListener);
                    return (T) query;
                }
            }
        };

        return StreamSupport.stream(queryIterator.spliterator(), false);
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    public List<Pattern> parsePatterns(String patternsString) {
        return parseQueryFragment(GraqlParser::patterns, QueryVisitor::visitPatterns, patternsString, getLexer(patternsString));
    }

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    public Pattern parsePattern(String patternString){
        return parseQueryFragment(GraqlParser::pattern, QueryVisitor::visitPattern, patternString, getLexer(patternString));
    }

    /**
     * Parse any part of a Graql query
     * @param parseRule a method on GraqlParser that yields the parse rule you want to use (e.g. GraqlParser::variable)
     * @param visit a method on QueryVisitor that visits the parse rule you specified (e.g. QueryVisitor::visitVariable)
     * @param queryString the string to parse
     * @param lexer
     * @param <T> The type the query is expected to parse to
     * @param <S> The type of the parse rule being used
     * @return the parsed result
     */
    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit, String queryString, GraqlLexer lexer
    ) {
        GraqlErrorListener errorListener = new GraqlErrorListener(queryString);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        return parseQueryFragment(parseRule, visit, tokens, errorListener);
    }

    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit,
            TokenStream tokens, GraqlErrorListener errorListener
    ) {
        GraqlParser parser = new GraqlParser(tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        return parseQueryFragment(parseRule, visit, parser, errorListener);
    }

    private <T, S extends ParseTree> T parseQueryFragment(
            Function<GraqlParser, S> parseRule, BiFunction<QueryVisitor, S, T> visit, GraqlParser parser,
            GraqlErrorListener errorListener) {

        S tree = null;

        try {
            tree = parseRule.apply(parser);
        } catch (ParseCancellationException e) {
            // ignore because we report errors right after this
        }

        if (errorListener.hasErrors()) {
            throw GraqlSyntaxException.parsingError(errorListener.toString());
        }

        // This should always be the case because if there is an error we throw
        assert tree != null;

        return visit.apply(getQueryVisitor(), tree);
    }

    private GraqlLexer getLexer(String queryString) {
        ANTLRInputStream input = new ANTLRInputStream(queryString);
        return new GraqlLexer(input);
    }

    private QueryVisitor getQueryVisitor() {
        ImmutableMap<String, Function<List<Object>, Aggregate>> immutableAggregates =
                ImmutableMap.copyOf(aggregateMethods);

        return new QueryVisitor(immutableAggregates, queryBuilder);
    }

    // Aggregate methods that include other aggregates, such as group are not necessarily safe at runtime.
    // This is unavoidable in the parser.
    @SuppressWarnings("unchecked")
    private void registerDefaultAggregates() {
        registerAggregate("count", 0, args -> Graql.count());
        registerAggregate("ask", 0, args -> Graql.ask());
        registerAggregate("sum", 1, args -> Aggregates.sum((Var) args.get(0)));
        registerAggregate("max", 1, args -> Aggregates.max((Var) args.get(0)));
        registerAggregate("min", 1, args -> Aggregates.min((Var) args.get(0)));
        registerAggregate("mean", 1, args -> Aggregates.mean((Var) args.get(0)));
        registerAggregate("median", 1, args -> Aggregates.median((Var) args.get(0)));
        registerAggregate("std", 1, args -> Aggregates.std((Var) args.get(0)));

        registerAggregate("group", 1, 2, args -> {
            if (args.size() < 2) {
                return Aggregates.group((Var) args.get(0));
            } else {
                return Aggregates.group((Var) args.get(0), (Aggregate) args.get(1));
            }
        });
    }
}
