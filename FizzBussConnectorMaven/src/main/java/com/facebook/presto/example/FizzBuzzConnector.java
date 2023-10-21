import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class FizzBuzzConnector implements Connector {
    private final NodeManager nodeManager;

    public FizzBuzzConnector(ConnectorContext context) {
        this.nodeManager = context.getNodeManager();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return new FizzBuzzTransactionHandle();
    }

    @Override
    public ConnectorSplitSource getSplitSource(ConnectorTransactionHandle transaction, SplitContext splitContext) {
        List<ConnectorSplit> splits = ImmutableList.of(new FizzBuzzSplit());
        return new FixedSplitSource(splits);
    }

    @Override
    public List<HostAddress> getNodeAddresses() {
        return nodeManager.getWorkerNodes().stream()
                .map(node -> HostAddress.fromParts(node.getHost(), node.getHttpUri().getPort()))
                .collect(toImmutableList());
    }

    @Override
    public Set<ConnectorTableHandle> getTableHandles(ConnectorTransactionHandle transactionHandle) {
        return ImmutableSet.of(new FizzBuzzTableHandle());
    }
}

