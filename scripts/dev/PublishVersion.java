import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import java.time.Instant;

/**
 * Dev tool: writes index.publishedVersion in Hazelcast by hand, to test Search
 * readiness before Indexing publishes for real. Run with the Hazelcast jar:
 *
 *   java -cp <hazelcast-5.5.0.jar> scripts/dev/PublishVersion.java <version> [address] [cluster]
 */
public class PublishVersion {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("usage: PublishVersion <version> [address=127.0.0.1:5701] [cluster=skimsearch]");
            System.exit(2);
        }
        long version = Long.parseLong(args[0]);
        String address = args.length > 1 ? args[1] : "127.0.0.1:5701";
        String cluster = args.length > 2 ? args[2] : "skimsearch";

        ClientConfig cc = new ClientConfig();
        cc.setClusterName(cluster);
        cc.getNetworkConfig().addAddress(address);
        cc.setProperty("hazelcast.logging.type", "none");

        var hz = HazelcastClient.newHazelcastClient(cc);
        try {
            hz.getMap("index-meta").put("index.publishedVersion", version);
            System.out.println("ts=" + Instant.now() + " event=published version=" + version);
        } finally {
            hz.shutdown();
        }
    }
}