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
package com.facebook.presto.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.example.MetadataUtil.CATALOG_CODEC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestExampleClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestExampleClient.class, "/example-data/example-metadata - Linda.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadata = metadataUrl.toURI();
        ExampleClient client = new ExampleClient(new ExampleConfig().setMetadata(metadata), CATALOG_CODEC);
        //assertEquals(client.getSchemaNames(), ImmutableSet.of("example", "tpch"));
        //Linda
        assertEquals(client.getSchemaNames(), ImmutableSet.of("example"));
        assertEquals(client.getTableNames("example"), ImmutableSet.of("example"));
        //assertEquals(client.getTableNames("tpch"), ImmutableSet.of("orders", "lineitem"));

        //Linda
       // ExampleTable table = client.getTable("example", "numbers");
        ExampleTable table = client.getTable("example");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "numbers");
        //Linda
        //assertEquals(table.getColumns(), ImmutableList.of(new ExampleColumn("text", createUnboundedVarcharType()), new ExampleColumn("value", BIGINT)));
        assertEquals(table.getColumns(), ImmutableList.of(new ExampleColumn("Id", BIGINT), new ExampleColumn("text", createUnboundedVarcharType())));
        //assertEquals(table.getSources(), ImmutableList.of(metadata.resolve("numbers-1.csv"), metadata.resolve("numbers-2.csv")));
        assertEquals(table.getSources(), ImmutableList.of(metadata.resolve("fz-ld-1.csv")));


    }
}
