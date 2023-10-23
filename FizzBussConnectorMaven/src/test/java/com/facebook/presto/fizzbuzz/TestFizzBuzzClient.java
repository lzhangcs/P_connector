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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.fizzbuzz.MetadataUtil.CATALOG_CODEC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestFizzBuzzClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestFizzBuzzClient.class, "/example-data/fizzbuzz-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadata = metadataUrl.toURI();
        FizzBuzzClient client = new FizzBuzzClient(new ExampleConfig().setMetadata(metadata), CATALOG_CODEC);

        //Linda
        assertEquals(client.getSchemaNames(), ImmutableSet.of("FizzBuzz"));
        assertEquals(client.getTableNames("FizzBuzz"), ImmutableSet.of("fizzBuzz"));


        //Linda
        FizzBuzzTable table = client.getTable("FizzBuzz");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "fizzBuzz");
        //Linda
        assertEquals(table.getColumns(), ImmutableList.of(new FizzBuzzColumn("Id", BIGINT), new FizzBuzzColumn("text", createUnboundedVarcharType())));
        assertEquals(table.getSources(), ImmutableList.of(metadata.resolve("fizzBuzz.csv")));


    }
}
