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


import HALParser from '../src/js/HAL/HALParser';
import * as MockedResponses from './modules/MockedEngineResponses';
import * as GlobalMocks from './modules/GlobalMocks';

beforeAll(() => {
  GlobalMocks.MockLocalStorage();
});

test('Parse single object HAL response with showIsa false', () => {
  const responseObj = MockedResponses.HALParserTestResponse0;
  const obj = HALParser.parseResponse(responseObj, false);
  expect(obj.nodes.length).toBe(1);
  expect(obj.edges.length).toBe(0);
  expect(obj.nodes[0].properties.id).toBe('4128');
});

test('Parse single object HAL response with showIsa true', () => {
  const responseObj = MockedResponses.HALParserTestResponse0;
  const obj = HALParser.parseResponse(responseObj, true);
  expect(obj.nodes.length).toBe(2);
  expect(obj.edges.length).toBe(1);
  expect(obj.nodes[0].properties.id).toBe('4128');
});

test('Parse HAL response with embedded', () => {
  const responseObj = MockedResponses.HALParserTestResponse1;
  const obj = HALParser.parseResponse(responseObj, true);
  expect(obj.nodes.length).toBe(7);
  expect(obj.edges.length).toBe(6);
  const collectedIds = obj.nodes.reduce((accumulator, current) => {
    const currentId = current.properties.id;
    let count = 1;
    if (currentId in accumulator) { count = accumulator[currentId] + 1; }
    return Object.assign(accumulator, { [currentId]: count });
  }, {});
  expect(collectedIds[984997984]).toBe(3);
  expect(collectedIds[3396772072]).toBe(1);
  expect(collectedIds[52064336]).toBe(1);
  expect(collectedIds[2285097056]).toBe(1);
  expect(collectedIds[35004512]).toBe(1);
});

test('Parse single object HAL response with reflexive relation', () => {
  const responseObj = MockedResponses.HALParserTestResponseReflexive;
  const obj = HALParser.parseResponse(responseObj, true);
  expect(obj.nodes.length).toBe(3);
  expect(obj.edges.length).toBe(2);
});

test('Parse empty HAL response', () => {
  const responseObj = [];
  const obj = HALParser.parseResponse(responseObj, false, false);
  expect(obj.nodes.length).toBe(0);
});
