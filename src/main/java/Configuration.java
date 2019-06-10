import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * The Configuration program implements an application that
 * simply creates a connection with Hadoop using zookeeper
 *
 * @author Bibek Chaudhary
 * @version 1.0
 * @since 2018-06-07
 */
public class Configuration {

    /**
     * Creates a new Connection for HBase using zookeeper
     *
     * @return
     * @throws IOException
     */
    public static Connection createConnection() throws IOException {

//        String hbaseZkQuorum="10.10.5.30,10.10.5.31,10.10.5.32,10.10.5.33";
        String hbaseZkQuorum="10.10.5.21,10.10.5.22,10.10.5.23,10.10.5.24,10.10.5.29";

        // Instantiating configuration class
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);

        Connection connection = ConnectionFactory.createConnection(conf);

        return connection;
    }

}
