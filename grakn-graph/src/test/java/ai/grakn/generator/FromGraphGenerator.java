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
 *
 */

package ai.grakn.generator;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeName;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static ai.grakn.generator.GraknGraphs.lastGeneratedGraph;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public abstract class FromGraphGenerator<T> extends AbstractGenerator<T> {
    FromGraphGenerator(Class<T> type) {
        super(type);
    }

    private boolean useLastGeneratedGraph = false;

    protected final GraknGraph graph() {
        if (useLastGeneratedGraph) {
            return lastGeneratedGraph();
        } else {
            return gen().make(GraknGraphs.class).setOpen(true).generate(random, status);
        }
    }

    protected final TypeName unusedName() {
        return gen().make(TypeNames.class).mustBeUnused().generate(random, status);
    }

    protected final <S extends FromGraphGenerator<?>> S genFromGraph(Class<S> generatorClass) {
        S generator = gen().make(generatorClass);
        if (useLastGeneratedGraph) generator.useLastGeneratedGraph();
        return generator;
    }

    public final void configure(FromGraph fromGraph) {
        useLastGeneratedGraph();
    }

    final void useLastGeneratedGraph() {
        useLastGeneratedGraph = true;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface FromGraph {
    }
}
