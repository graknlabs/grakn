/*
 * Copyright (C) 2020 Grakn Labs
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


package grakn.core.test.behaviour.config;

import grakn.core.kb.server.Transaction;
import io.cucumber.java.DataTableType;
import io.cucumber.java.ParameterType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class Parameters {

    @ParameterType("true|false")
    public Boolean bool(String bool) {
        return Boolean.parseBoolean(bool);
    }

    @ParameterType("[0-9]+")
    public Integer number(String number) {
        return Integer.parseInt(number);
    }

    @ParameterType("read|write")
    public Transaction.Type transaction_type(String type){
        if (type.equals("read")) {
            return Transaction.Type.READ;
        } else {
            return Transaction.Type.WRITE;
        }
    }

    @DataTableType
    public List<Transaction.Type> transaction_types(List<String> values) {
        List<Transaction.Type> typeList = new ArrayList<>();
        for (String value : values) {
            Transaction.Type type = transaction_type(value);
            assertNotNull(type);
            typeList.add(type);
        }
        return typeList;
    }
}
