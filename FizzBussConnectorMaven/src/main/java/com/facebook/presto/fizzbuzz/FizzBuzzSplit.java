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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;

//Linda
import com.google.common.collect.ImmutableMap;
import javax.inject.Inject;
import java.util.HashMap;



import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class FizzBuzzSplit
        implements ConnectorSplit {
    //private final String connectorId;
    private final String schemaName;
    private final String tableName;
    //private final URI uri;

    //Linda
    private immutableMap <Integer, String> fbImmutableMap;


    @JsonCreator
    public FizzBuzzSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("uri") URI uri)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.uri = requireNonNull(uri, "uri is null");

        addresses = ImmutableList.of(HostAddress.fromUri(uri));
    }

    public static void writeMap2CsvFile(HashMap<Integer, String> map, String filepath)
    {
        final String[] header = new String[] { "id", "group"};
        String eol = System.getProperty("line.separator");

        try (FileWriter writer = new FileWriter(filepath, false))
        {
            writer.append(header[0]).append(',').append(header[1]).append(eol);
            for (Map.Entry<Integer, String> entry : map.entrySet())
            {
                writer.append(Integer.toString(entry.getKey())).append(',').append(entry.getValue()).append(eol);
            }
        }
        catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    //linda
    public immutableMap<Integer, String> generateFizzBuzzData() {
        HashMap<Integer, String> result = new HashMap<>();

        for (int i = 1; i <= 10000; i++) {
            if (i % 3 == 0 && i % 5 == 0) {
                result.put(i, "FizzBuzz");
            } else if (i % 3 == 0) {
                result.put(i, "Fizz");
            } else if (i % 5 == 0) {
                result.put(i, "Buzz");
            } else {
                result.put(i, Integer.toString(i));
            }
        }

        String csvfile = "C:\\temp\\fizzBuzz.csv";
        writeMap2CsvFile(result, csvfile);

        fbImmutableMap = ImmutableMap.copyOf(result);
        return fbImmutablemap;
    }
    //linda
    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
