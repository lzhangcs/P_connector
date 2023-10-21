import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SplitContext;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class FizzBuzzSplit implements ConnectorSplit {
    private final List<String> fizzBuzzData;

    public FizzBuzzSplit() {
        fizzBuzzData = generateFizzBuzzData();
    }

    private List<String> generateFizzBuzzData() {
        List<String> result = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            if (i % 3 == 0 && i % 5 == 0) {
                result.add("FizzBuzz");
            } else if (i % 3 == 0) {
                result.add("Fizz");
            } else if (i % 5 == 0) {
                result.add("Buzz");
            } else {
                result.add(String.valueOf(i));
            }
        }
        return result;
    }

    @Override
    public List<HostAddress> getAddresses() {
        // This can return the addresses of Presto worker nodes if necessary
        return ImmutableList.of();
    }
}






