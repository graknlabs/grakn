/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.answer;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptImpl;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Mapped;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import graql.lang.pattern.variable.Reference;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static grakn.core.reasoner.resolution.answer.AnswerState.Partial.Identity.identity;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class AnswerStateTest {

    @Test
    public void test_initial_empty_mapped_to_downstream_and_back() {
        Map<Reference.Name, Reference.Name> mapping = new HashMap<>();
        mapping.put(Reference.name("a"), Reference.name("x"));
        mapping.put(Reference.name("b"), Reference.name("y"));
        Mapped mapped = Top.initial(mapping.keySet(), false, null, null).toDownstream().mapToDownstream(Mapping.of(mapping));
        assertTrue(mapped.conceptMap().concepts().isEmpty());

        Map<Reference.Name, Concept> concepts = new HashMap<>();
        concepts.put(Reference.name("x"), new MockConcept(0));
        concepts.put(Reference.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(concepts), null);
        Map<Reference.Name, Concept> expected = new HashMap<>();
        expected.put(Reference.name("a"), new MockConcept(0));
        expected.put(Reference.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expected), partial.conceptMap());
    }

    @Test
    public void test_initial_partially_mapped_to_downstream_and_back() {
        Map<Reference.Name, Reference.Name>  mapping = new HashMap<>();
        mapping.put(Reference.name("a"), Reference.name("x"));
        mapping.put(Reference.name("b"), Reference.name("y"));
        Map<Reference.Name, Concept> concepts = new HashMap<>();
        concepts.put(Reference.name("a"), new MockConcept(0));
        Top top = Top.initial(concepts.keySet(), false, null, null);
        Mapped mapped = identity(new ConceptMap(concepts), top,  null, false)
                .mapToDownstream(Mapping.of(mapping));

        Map<Reference.Name, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Reference.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Reference.Name, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Reference.name("x"), new MockConcept(0));
        downstreamConcepts.put(Reference.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(downstreamConcepts), null);

        Map<Reference.Name, Concept> expectedWithInitial = new HashMap<>();
        expectedWithInitial.put(Reference.name("a"), new MockConcept(0));
        expectedWithInitial.put(Reference.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expectedWithInitial), partial.conceptMap());
    }

    @Test
    public void test_initial_with_unmapped_elements() {
        Map<Reference.Name, Reference.Name>  mapping = new HashMap<>();
        mapping.put(Reference.name("a"), Reference.name("x"));
        mapping.put(Reference.name("b"), Reference.name("y"));
        Map<Reference.Name, Concept> concepts = new HashMap<>();
        concepts.put(Reference.name("a"), new MockConcept(0));
        concepts.put(Reference.name("c"), new MockConcept(2));
        Top top = Top.initial(concepts.keySet(), false, null, null);
        Mapped mapped = identity(new ConceptMap(concepts), top,  null, false)
                .mapToDownstream(Mapping.of(mapping));

        Map<Reference.Name, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Reference.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Reference.Name, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Reference.name("x"), new MockConcept(0));
        downstreamConcepts.put(Reference.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(downstreamConcepts), null);

        Map<Reference.Name, Concept> expectedWithInitial = new HashMap<>();
        expectedWithInitial.put(Reference.name("a"), new MockConcept(0));
        expectedWithInitial.put(Reference.name("b"), new MockConcept(1));
        expectedWithInitial.put(Reference.name("c"), new MockConcept(2));
        assertEquals(new ConceptMap(expectedWithInitial), partial.conceptMap());
    }

    public static class MockConcept extends ConceptImpl implements Concept {
        private final long id;

        public MockConcept(long id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "MockConcept{" +
                    "id='" + id + '\'' +
                    '}';
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public void delete() {
            // noop
        }

        @Override
        public GraknException exception(GraknException exception) {
            return exception;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MockConcept that = (MockConcept) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
