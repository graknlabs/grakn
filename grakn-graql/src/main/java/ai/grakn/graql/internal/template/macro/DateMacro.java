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

package ai.grakn.graql.internal.template.macro;

import ai.grakn.graql.macro.Macro;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.List;

/**
 * <p>
 * Parse the given value (arg1) using the format (arg2).
 * Returns a String with the value of the date parsed into {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} which
 * is the date format that Graql accepts.
 *
 * Usage:
 *      {@literal @}date("01/30/2017", "mm/dd/yyyy")
 * </p>
 *
 * @author alexandraorth
 */
public class DateMacro implements Macro<Unescaped<String>> {

    @Override
    public Unescaped<String> apply(List<Object> values) {
        if(values.size() != 2){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        String originalDate = values.get(0).toString();
        String originalFormat = values.get(1).toString();

        return Unescaped.of(convertDateFormat(originalDate, originalFormat));
    }

    @Override
    public String name() {
        return "date";
    }

    private String convertDateFormat(String originalDate, String originalFormat){
        originalFormat = removeQuotes(originalFormat);

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(originalFormat);

            TemporalAccessor parsedDate = formatter.parseBest(
                    originalDate, LocalDateTime::from, LocalDate::from, LocalTime::from);

            return extractLocalDateTime(parsedDate).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (IllegalArgumentException e){
            throw new IllegalArgumentException("Cannot parse date format " + originalFormat + ". See DateTimeFormatter#ofPattern");
        } catch (DateTimeParseException e){
            throw new DateTimeParseException("Cannot parse date value " + originalDate + " with format " + originalFormat, e.getParsedString(), e.getErrorIndex());
        }
    }

    /**
     * Extract a {@link LocalDateTime} object from a {@link TemporalAccessor}.
     * If the given date is a {@link LocalDate}, sets the response to the start of that day.
     * If the given date is a {@link LocalTime}, sets the response to the current day.
     *
     * @param parsedDate The parsed date to convert.
     * @return A {@link LocalDateTime} object containing a formatted date.
     */
    private LocalDateTime extractLocalDateTime(TemporalAccessor parsedDate){
        if(parsedDate instanceof LocalDate){
            return ((LocalDate) parsedDate).atStartOfDay();
        } else if(parsedDate instanceof LocalTime){
            return ((LocalTime) parsedDate).atDate(LocalDate.now());
        } else {
            return LocalDateTime.from(parsedDate);
        }
    }

    private String removeQuotes(String str){
        return str.replace("\"", "");
    }
}
