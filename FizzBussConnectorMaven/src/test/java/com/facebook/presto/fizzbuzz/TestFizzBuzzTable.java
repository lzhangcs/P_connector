/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.fizzbuzz;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.fizzbuzz.MetadataUtil.TABLE_CODEC;
import static org.testng.Assert.assertEquals;

public class TestFizzBuzzTable
{
    private final FizzBuzzTable FizzBuzzTable = new FizzBuzzTable("tableName",
            ImmutableList.of(new FizzBuzzColumn("a", createUnboundedVarcharType()), new FizzBuzzColumn("b", BIGINT)),
            ImmutableList.of(URI.create("file://fizzbuzz-metadata.json")));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(FizzBuzzTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("a", createUnboundedVarcharType()),
                new ColumnMetadata("b", BIGINT)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = TABLE_CODEC.toJson(FizzBuzzTable);
        FizzBuzzTable FizzBuzzTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(FizzBuzzTableCopy.getName(), FizzBuzzTable.getName());
        assertEquals(FizzBuzzTableCopy.getColumns(), FizzBuzzTable.getColumns());
        assertEquals(FizzBuzzTableCopy.getSources(), FizzBuzzTable.getSources());
    }
}
