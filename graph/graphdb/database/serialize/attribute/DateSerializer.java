/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
//
//package grakn.core.graph.graphdb.database.serialize.attribute;
//
//import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;
//import grakn.core.graph.diskstorage.ScanBuffer;
//import grakn.core.graph.diskstorage.WriteBuffer;
//import grakn.core.graph.graphdb.database.serialize.OrderPreservingSerializer;
//import grakn.core.graph.graphdb.database.serialize.attribute.LongSerializer;
//
//import java.text.ParseException;
//import java.util.Date;
//
////public class DateSerializer implements OrderPreservingSerializer<Date> {
//
//    private final LongSerializer ls = LongSerializer.INSTANCE;
//    private final StdDateFormat dateFormat = StdDateFormat.instance;
//
//    @Override
//    public Date read(ScanBuffer buffer) {
//        long utc = ls.read(buffer);
//        return new Date(utc);
//    }
//
//    @Override
//    public void write(WriteBuffer out, Date attribute) {
//        long utc = attribute.getTime();
//        ls.write(out, utc);
//    }
//
//    @Override
//    public Date readByteOrder(ScanBuffer buffer) {
//        return read(buffer);
//    }
//
//    @Override
//    public void writeByteOrder(WriteBuffer buffer, Date attribute) {
//        write(buffer,attribute);
//    }
//
//    @Override
//    public Date convert(Object value) {
//        if (value instanceof Number && !(value instanceof Float) && !(value instanceof Double)) {
//            return new Date(((Number)value).longValue());
//        } else if (value instanceof String) {
//            try {
//                return dateFormat.parse((String) value);
//            } catch (ParseException ignored) {
//            }
//        }
//        return null;
//    }
//}
