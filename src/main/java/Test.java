import com.sun.tools.hat.internal.util.Misc;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

//import misc;

public class Test {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();

        Connection connection = configuration.createConnection();

        Table table = connection.getTable(TableName.valueOf("SYSTEM.CATALOG"));


        // Instantiating Scan class
        Scan scan = new Scan();

        // Scanning requires column names
        scan.addFamily(Bytes.toBytes("0"));
//        scan.addColumn(Bytes.toBytes("0"), Bytes.toBytes("COLUMN_QUALIFIER"));

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Reading values from scan result
        for (Result result : scanner) {
//            System.out.println("Found row: " + result);
//            byte[] b = result.getValue(Bytes.toBytes("0"), Bytes.toBytes("COLUMN_QUALIFIER"));

            List<Cell> cells = result.listCells();

            for (Cell c : cells) {
                byte columnFamily[] = CellUtil.cloneFamily(c);
                byte qualifier[] = CellUtil.cloneQualifier(c);
                byte value[] = CellUtil.cloneValue(c);

                System.out.println("columnFamily:" + Bytes.toString(columnFamily) +
                        " qualifier:" + Bytes.toString(qualifier));

                for(byte b: value){
                    System.out.println("value: "+Byte.toString(b));
                }


//                System.out.println("columnFamily:" + Bytes.toString(columnFamily) +
//                        " qualifier:" + Bytes.toString(qualifier) + " value:" + Bytes.toString(value));

            }

            //closing the scanner
            scanner.close();

            // Closing Table
            table.close();
            connection.close();

        }
//
//        // Instantiating Get class
//        Get get = new Get(Bytes.toBytes(""));
//
//        // Reading the data
//        Result result = table.get(get);
//
//        // Reading values from Result class object
//        byte [] value1 = result.getValue(Bytes.toBytes("0"),Bytes.toBytes("COLUMN_QUALIFIER"));
//
//        System.out.println(Bytes.toString(value1));


//        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
//        HTableDescriptor[] hTableDescriptors = admin.listTables();
//
//        for(HTableDescriptor tableDescriptor: hTableDescriptors){
//            String table_name = tableDescriptor.getNameAsString();
//            System.out.println("Table names: "+ table_name);
//        }


//        String hbaseZkQuorum="10.10.5.30,10.10.5.31,10.10.5.32,10.10.5.33";
//
//        // Instantiating configuration class
//        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//
//        conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
//        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
//
//        // Instantiating HTable class
//        HTable table1 = new HTable(conf, "SYSTEM.CATALOG");
//
//        // Instantiating Get class
//        Get g = new Get(Bytes.toBytes("row1"));
//
//        // Reading the data
//        Result result = table1.get(g);
//
//        // Reading values from Result class object
//        byte [] value = result.getValue(Bytes.toBytes("0"),Bytes.toBytes("COLUMN_QUALIFIER"));
//
//        // Printing the values
//        String name = Bytes.toString(value);
//
//        System.out.println("name: " + name);

    }
}
