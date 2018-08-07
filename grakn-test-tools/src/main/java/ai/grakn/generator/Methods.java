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

package ai.grakn.generator;

import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;
import org.mockito.Mockito;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.stream.Stream;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A generator that produces random {@link Method}s of a given {@link Class}
 *
 * @author Felix Chapman
 */
public class Methods extends AbstractGenerator<Method> {

    private Class<?> clazz = null;

    public Methods() {
        super(Method.class);
    }

    @Override
    protected Method generate() {
        if (clazz == null) throw new IllegalStateException("Must use annotation MethodOf");

        return random.choose(clazz.getMethods());
    }

    @SuppressWarnings("unused") /** Used through {@link MethodOf} annotation*/
    public void configure(@SuppressWarnings("unused") MethodOf methodOf) {
        this.clazz = methodOf.value();
    }

    public static Object[] mockParamsOf(Method method) {
        return Stream.of(method.getParameters()).map(Parameter::getType).map(Methods::mock).toArray();
    }

    private static <T> T mock(Class<T> clazz) {
        if (clazz.equals(boolean.class) || clazz.equals(Object.class)) {
            return (T) Boolean.FALSE;
        } else if (clazz.equals(String.class)) {
            return (T) "";
        } else {
            return Mockito.mock(clazz);
        }
    }

    /**
     * Specify what class to generate methods from
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface MethodOf {
        Class<?> value();
    }
}
