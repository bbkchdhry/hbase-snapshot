import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;

/**
 * The Snapshots program implements an application that
 * simply creates a snapshot of HBase and stores it in HDFS
 *
 * @author Bibek Chaudhary
 * @version 1.0
 * @since 2018-06-07
 */
public class Snapshots {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String date = sdf.format(new Date());

    private static Logger log = Logger.getLogger(Snapshots.class.getName());

    /**
     * Zips Today's hbase snapshot using tar and removes '.hbase-snapshot' folder from local system.
     * Calls copyToHDFS() function after snapshots are zipped successfully
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void zip_t() throws IOException, InterruptedException {

        String[] command = { "bash", "-c", "cd /home/saque/hbase-snapshots/"+ date +
                " && tar -czvf hbase-snapshot.tar.gz .hbase-snapshot archive && rm -rf .hbase-snapshot archive"};


        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" Successfully zipped hbase snapshots");
            copyToHDFS();
        }else {
            log.error(" Failed to zip snapshots");
        }
    }

    /**
     * Copies snapshots from local system to HDFS and removes from local system.
     * If successful it calls remove_old_snapshots() function
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void copyToHDFS() throws IOException, InterruptedException {

        String[] command = { "bash", "-c", "/home/saque/hadoopec/hadoop/bin/hadoop fs -rm -r /hbase-snapshots/" + date +
                " && cd /home/saque/hbase-snapshots/" +
                " && /home/saque/hadoopec/hadoop/bin/hadoop fs -put " + date + " /hbase-snapshots" +
                " && rm -rf " + date};

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" Successfully Stored hbase snapshots to HDFS");

            /**********remove yesterday's snapshots**********/
            remove_old_snapshots();
        }else {
            log.error(" Failed to store snapshots in HDFS from local");
        }
    }

    /**
     * Deletes yesterday's snapshot from HDFS
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void remove_old_snapshots() throws IOException, InterruptedException {
        String four_days_before = LocalDate.parse(date).minusDays(4).toString();
        String[] command = {"bash", "-c", "/home/saque/hadoopec/hadoop/bin/hadoop fs -rm -r /hbase-snapshots/"+ four_days_before};
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" Old Snapshot Deleted Successfully");
        }else {
            log.error(" Failed to Delete Old Snapshot");
        }
    }

    /**
     * Creates snapshot of all HBase tables
     *
     * @param connection
     * @throws IOException
     */
    public void createSnapshot(Connection connection) throws IOException {
        // Instantiating HbaseAdmin class
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        HTableDescriptor[] hTableDescriptors = admin.listTables();

        for(HTableDescriptor tableDescriptor: hTableDescriptors){
            String table_name = tableDescriptor.getNameAsString();

            // Creating a Snapshot for table
            System.out.println("Table names: "+ table_name);
            String snapshot_name = table_name+"_SNAPSHOT";
            admin.snapshot(snapshot_name, table_name);
        }

        List snapshots = admin.listSnapshots();
        if(snapshots.isEmpty()){
            log.error(" Snapshots cannot be created");

        }else{
            log.info(" Snapshots created successfully");
        }
    }

    /**
     * Get Snapshot from hdfs to local file system and calls zip_t() function
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void getSnapshot() throws IOException, InterruptedException {

        String[] command = {"bash", "-c", "/home/saque/hadoopec/hadoop/bin/hadoop fs -get /hbase-snapshots/" + date };

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" Snapshot Copied Successfully in local file system");

            /**********zip today's snapshots**********/
            zip_t();

        }else {
            log.error(" Failed to Copy Snapshot");
        }
    }

    /**
     * Exports HBase Snapshots in HDFS for backup and to create archive files
     *
     * @param snapshot_name
     * @throws IOException
     * @throws InterruptedException
     */
    public void exportSnapshot(String snapshot_name) throws IOException, InterruptedException {
        String[] command = {"bash", "-c", "/home/saque/hadoopec/hbase/bin/hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot " +
                snapshot_name + " -copy-to /hbase-snapshots/" + date + " -mappers 20"};

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" " + snapshot_name + " Exported Successfully");

        }else {
            log.error(" Failed to Export Snapshot => " + snapshot_name);
        }
    }

    /**
     * Lists all snapshots of HBase table
     *
     * @param connection
     * @throws IOException
     * @throws InterruptedException
     */
    public void listSnapshot(Connection connection) throws IOException, InterruptedException {

        String[] command = {"bash", "-c", "/home/saque/hadoopec/hadoop/bin/hadoop fs -mkdir /hbase-snapshots/"+ date };

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        if(process.waitFor()==0){
            log.info(" Folder created successfully to store snapshots in hdfs");

        }else {
            log.error(" Failed to create a new folder to store snapshots in hdfs");
        }

        System.out.println("\n********************Listing snapshots********************\n");

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();

        for(HBaseProtos.SnapshotDescription snapshot : snapshots){
            String snapshot_name = snapshot.getName();

            System.out.println(snapshot_name);

            /*********************************Export Snapshots in HDFS for backup*********************************/
            exportSnapshot(snapshot_name);

        }
    }

    /**
     * Deletes All Snapshot from HBase using regex
     *
     * @param connection
     * @throws IOException
     */
    public void deleteAllSnapshot(Connection connection) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        admin.deleteSnapshots("\\w.*");

        List snapshots = admin.listSnapshots();
        if(snapshots.isEmpty()){
            log.info(" Snapshots in hdfs deleted successfully");
        }else{
            log.error(" Snapshots in hdfs cannot be deleted");
        }

    }

    /**
     * Main method to call all the functions to create HBase snapshot and store it in HDFS
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        Snapshots snapshots = new Snapshots();

        Connection connection = Configuration.createConnection();

        /*********************************Create Snapshot of all Hbase table*********************************/
        snapshots.createSnapshot(connection);

        /*********************************List Snapshots*********************************/
        snapshots.listSnapshot(connection);

        /*********************************Get Snapshot from hdfs*********************************/
        snapshots.getSnapshot();

        /*********************************Delete All Snapshots*********************************/
        snapshots.deleteAllSnapshot(connection);

        connection.close();
    }
}
