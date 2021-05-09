package br.com.danielamaral;

import br.com.danielamaral.model.SecurityHistItem;
import br.com.danielamaral.util.Constants;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.*;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class DemoBigTable {

    private final static List<SecurityHistItem> histData = new ArrayList<>();

    public static void main(String[] args) {
        BigtableTableAdminClient adminClient = null;
        BigtableDataClient dataClient = null;
        try {
            adminClient = createAdminClient();
            dataClient = createDataClient();

            System.out.println("## Creating table");
            createTable(adminClient);

            System.out.println("## Creating Column Family");
            createColumnFamily(adminClient);

            System.out.println("## Reading Table MetaData");
            getTableMeta(adminClient);

            System.out.println("## Reading file");
            readFile();

            System.out.println("## Loading data");
            writeBatch(dataClient);

            System.out.println("## Reading record by key");
            getRecordsByKey(dataClient, "AMZN#20210507T00:00:00");

            System.out.println("## Reading records by key range");
            getRecordsByKeyRange(dataClient, "AMZN#20210501T00:00:00", "AMZN#20210507T00:00:00");

        } finally {
            if (adminClient != null) adminClient.close();
            if (dataClient != null) dataClient.close();
        }
    }

    private static void createTable(BigtableTableAdminClient adminClient) {
        try {
            adminClient.createTable(
                    CreateTableRequest.of(Constants.BT_TABLE_ID)
                            .addFamily(Constants.BT_TABLE_COLUMN_FAMILY)
            );
        } catch (Exception e) {
            System.out.println("Error createTable");
            e.printStackTrace();
        }
    }

    private static void createColumnFamily(BigtableTableAdminClient adminClient) {
        System.out.printf("%nCreating column family %s with max versions GC rule%n", Constants.BT_TABLE_COLUMN_FAMILY);
        GCRules.VersionRule versionRule = GCRULES.maxVersions(1);

        try {
            ModifyColumnFamiliesRequest columnFamiliesRequest = ModifyColumnFamiliesRequest.of(Constants.BT_TABLE_ID).updateFamily(Constants.BT_TABLE_COLUMN_FAMILY, versionRule);
            adminClient.modifyFamilies(columnFamiliesRequest);
            System.out.println("Created column family: " + Constants.BT_TABLE_COLUMN_FAMILY);
        } catch (AlreadyExistsException e) {
            System.out.println("Error createColumnFamily");
            e.printStackTrace();
        }
    }

    private static void getRecordsByKeyRange(BigtableDataClient dataClient, String start, String end) {

        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests. After completing all of your requests, call
        // the "close" method on the client to safely clean up any remaining background resources.
        try {
            Query query = Query.create(Constants.BT_TABLE_ID).range(start, end);
            ServerStream<Row> rows = dataClient.readRows(query);

            for (Row row : rows) {
                printRow(row);
            }
        } catch (Exception e) {
            System.out.println("Error getRecordsByKeyRange");
            e.printStackTrace();
        }
    }

    private static void getRecordsByKey(BigtableDataClient dataClient, String key) {

        try {

            Row row = dataClient.readRow(Constants.BT_TABLE_ID, key);
            printRow(row);

        } catch (Exception e) {
            System.out.println("Error getRecordsByKey");
            e.printStackTrace();
        }
    }


    public static void getTableMeta(BigtableTableAdminClient adminClient) {
        System.out.println("\nPrinting table metadata");
        try {
            Table table = adminClient.getTable(Constants.BT_TABLE_ID);
            System.out.println("Table: " + table.getId());
            Collection<ColumnFamily> columnFamilies = table.getColumnFamilies();
            for (ColumnFamily columnFamily : columnFamilies) {
                System.out.printf(
                        "Column family: %s%nGC Rule: %s%n",
                        columnFamily.getId(), columnFamily.getGCRule().toString());
            }
        } catch (NotFoundException e) {
            System.out.println("Error getTableMeta");
            e.printStackTrace();
        }
    }

    private static void readFile() {

        try (BufferedReader br = new BufferedReader(new FileReader("HistoricalDataToLoad.csv"))) {
            String line;


            //skip first file first line
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                histData.add(new SecurityHistItem(values[0], values[1], values[2], values[3], values[4], values[5], values[6]));
            }
            System.out.println(histData);
        } catch (IOException e) {
            System.out.println("Error readFile");
            e.printStackTrace();
        }
    }

    private static BigtableDataClient createDataClient() {
        BigtableDataClient dataClient = null;
        try {
            dataClient = BigtableDataClient.create(Constants.BT_PROJECT_ID, Constants.BT_INSTANCE_ID);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataClient;
    }

    public static BigtableTableAdminClient createAdminClient() {
        try {
            // Creates the settings to configure a bigtable table admin client.
            BigtableTableAdminSettings adminSettings =
                    BigtableTableAdminSettings.newBuilder()
                            .setProjectId(Constants.BT_PROJECT_ID)
                            .setInstanceId(Constants.BT_INSTANCE_ID)
                            .build();

            // Creates a bigtable table admin client.
            return BigtableTableAdminClient.create(adminSettings);
        } catch (IOException e) {
            System.out.println("Error createAdmin");
            e.printStackTrace();
        }
        return null;
    }

    public static void writeBatch(BigtableDataClient dataClient) {


        try {
            BulkMutation bulkMutation = BulkMutation.create(Constants.BT_TABLE_ID);

            for (SecurityHistItem sec : histData) {
                bulkMutation.add(
                        sec.getSecurity().concat("#").concat(sec.getTime()),
                        Mutation.create()
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_SECURITY,
                                        sec.getSecurity())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_TIME,
                                        sec.getTime())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_CLOSE,
                                        sec.getClose())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_VOLUME,
                                        sec.getVolume())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_OPEN,
                                        sec.getOpen())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_HIGH,
                                        sec.getHigh())
                                .setCell(
                                        Constants.BT_TABLE_COLUMN_FAMILY,
                                        Constants.BT_COL_LOW,
                                        sec.getLow())
                );
            }

            dataClient.bulkMutateRows(bulkMutation);


        } catch (Exception e) {
            System.out.println("Error writeBatch");
            e.printStackTrace();
        }
    }

    private static void printRow(Row row) {
        System.out.printf("Reading data for %s%n", row.getKey().toStringUtf8());
        String colFamily = "";
        for (RowCell cell : row.getCells()) {
            if (!cell.getFamily().equals(colFamily)) {
                colFamily = cell.getFamily();
                System.out.printf("Column Family %s%n", colFamily);
            }
            System.out.printf(
                    "\t%s: %s @%s%n",
                    cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8(), cell.getTimestamp());
        }
        System.out.println();
    }

}
