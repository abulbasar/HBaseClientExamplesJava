package com.example.data.hbase;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseHelper implements Closeable {

    private static Logger log = LoggerFactory.getLogger(HBaseHelper.class.getName());

    private static HBaseHelper helper = null;

    private Configuration configuration = null;
    private Connection connection = null;
    private Admin admin = null;
    private Map<String, Table> tablesByName = new HashMap<>();

    private HBaseHelper(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.connection = ConnectionFactory.createConnection(configuration);
        this.admin = connection.getAdmin();
    }

    public static HBaseHelper getInstance(Configuration configuration) {
        if(helper == null){
            try {
                helper = new HBaseHelper(configuration);
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        return helper;
    }

    public static HBaseHelper getInstance(){
        Configuration configuration = HBaseConfiguration.create();
        String path = HBaseHelper.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        configuration.addResource(new Path(path));
        return getInstance(configuration);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public List<String> listNamespaces() throws IOException{
        List<String> names = new ArrayList<>();
        NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
        for(int i=0;i<namespaces.length;++i){
            names.add(namespaces[i].getName());
        }
        return names;
    }

    public List<String> listTables(String namespace) throws IOException{

        List<String> names = new ArrayList<>();

        if(namespace.equalsIgnoreCase("<all>")){
            for(HTableDescriptor table: admin.listTables()){
                names.add(table.getTableName().getNameAsString());
            }
        }else {
            for(TableName table:  admin.listTableNamesByNamespace(namespace)){
                names.add(table.getNameAsString());
            }
        }
        log.info(String.format("Number of tables: %d", names.size()));

        return names;
    }

    public void createNamespace(String namespace) throws IOException {
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(nd);
    }

    public void dropNamespace(String namespace, boolean force) throws IOException {
        if (force) {
            TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
            for (TableName name : tableNames) {
                admin.disableTable(name);
                admin.deleteTable(name);
            }
        }
        admin.deleteNamespace(namespace);
    }

    public boolean existsTable(String table)
            throws IOException {
        return existsTable(TableName.valueOf(table));
    }

    public boolean existsTable(TableName table)
            throws IOException {
        return admin.tableExists(table);
    }

    public void createTable(String table, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, null, colfams);
    }

    public void createTable(TableName table, String... colfams)
            throws IOException {
        createTable(table, 1, null, colfams);
    }

    public void createTable(String table, int maxVersions, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), maxVersions, null, colfams);
    }

    public void createTable(TableName table, int maxVersions, String... colfams)
            throws IOException {
        createTable(table, maxVersions, null, colfams);
    }

    public void createTable(String table, byte[][] splitKeys, String... colfams)
            throws IOException {
        createTable(TableName.valueOf(table), 1, splitKeys, colfams);
    }

    public void createTable(TableName table, int maxVersions, byte[][] splitKeys,
                            String... colfams)
            throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table);
        for (String cf : colfams) {
            HColumnDescriptor coldef = new HColumnDescriptor(cf);
            coldef.setMaxVersions(maxVersions);
            desc.addFamily(coldef);
        }
        if (splitKeys != null) {
            admin.createTable(desc, splitKeys);
        } else {
            admin.createTable(desc);
        }
    }

    public void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    public void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    public void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    public void dropTable(TableName table) throws IOException {
        if (existsTable(table)) {
            if (admin.isTableEnabled(table)) disableTable(table);
            admin.deleteTable(table);
        }
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, false, colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, -1,
                setTimestamp, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, -1, setTimestamp, colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, false, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, String... colfams)
            throws IOException {
        fillTable(table, startRow, endRow, numCols, pad, setTimestamp, false,
                colfams);
    }

    public void fillTable(String table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, boolean random,
                          String... colfams)
            throws IOException {
        fillTable(TableName.valueOf(table), startRow, endRow, numCols, pad,
                setTimestamp, random, colfams);
    }

    public void fillTable(TableName table, int startRow, int endRow, int numCols,
                          int pad, boolean setTimestamp, boolean random,
                          String... colfams)
            throws IOException {
        Table tbl = connection.getTable(table);
        Random rnd = new Random();
        for (int row = startRow; row <= endRow; row++) {
            for (int col = 1; col <= numCols; col++) {
                Put put = new Put(Bytes.toBytes("row-" + padNum(row, pad)));
                for (String cf : colfams) {
                    String colName = "col-" + padNum(col, pad);
                    String val = "val-" + (random ?
                            Integer.toString(rnd.nextInt(numCols)) :
                            padNum(row, pad) + "." + padNum(col, pad));
                    if (setTimestamp) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col,
                                Bytes.toBytes(val));
                    } else {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName),
                                Bytes.toBytes(val));
                    }
                }
                tbl.put(put);
            }
        }
        tbl.close();
    }

    public void fillTableRandom(String table,
                                int minRow, int maxRow, int padRow,
                                int minCol, int maxCol, int padCol,
                                int minVal, int maxVal, int padVal,
                                boolean setTimestamp, String... colfams)
            throws IOException {
        fillTableRandom(TableName.valueOf(table), minRow, maxRow, padRow, minCol,
                maxCol, padCol, minVal, maxVal, padVal, setTimestamp, colfams);
    }

    public void fillTableRandom(TableName table,
                                int minRow, int maxRow, int padRow,
                                int minCol, int maxCol, int padCol,
                                int minVal, int maxVal, int padVal,
                                boolean setTimestamp, String... colfams)
            throws IOException {
        Table tbl = connection.getTable(table);
        Random rnd = new Random();
        int maxRows = minRow + rnd.nextInt(maxRow - minRow);
        for (int row = 0; row < maxRows; row++) {
            int maxCols = minCol + rnd.nextInt(maxCol - minCol);
            for (int col = 0; col < maxCols; col++) {
                int rowNum = rnd.nextInt(maxRow - minRow + 1);
                Put put = new Put(Bytes.toBytes("row-" + padNum(rowNum, padRow)));
                for (String cf : colfams) {
                    int colNum = rnd.nextInt(maxCol - minCol + 1);
                    String colName = "col-" + padNum(colNum, padCol);
                    int valNum = rnd.nextInt(maxVal - minVal + 1);
                    String val = "val-" + padNum(valNum, padCol);
                    if (setTimestamp) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), col,
                                Bytes.toBytes(val));
                    } else {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName),
                                Bytes.toBytes(val));
                    }
                }
                tbl.put(put);
            }
        }
        tbl.close();
    }

    public String padNum(int num, int pad) {
        String res = Integer.toString(num);
        if (pad > 0) {
            while (res.length() < pad) {
                res = "0" + res;
            }
        }
        return res;
    }

    public void put(String table, String row, String fam, String qual,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, val);
    }

    public void put(TableName table, String row, String fam, String qual,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    public void put(String table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        put(TableName.valueOf(table), row, fam, qual, ts, val);
    }

    public void put(TableName table, String row, String fam, String qual, long ts,
                    String val) throws IOException {
        Table tbl = connection.getTable(table);
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts,
                Bytes.toBytes(val));
        tbl.put(put);
        tbl.close();
    }

    public void put(String table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        put(TableName.valueOf(table), rows, fams, quals, ts, vals);
    }

    public void put(TableName table, String[] rows, String[] fams, String[] quals,
                    long[] ts, String[] vals) throws IOException {
        Table tbl = connection.getTable(table);
        for (String row : rows) {
            Put put = new Put(Bytes.toBytes(row));
            for (String fam : fams) {
                int v = 0;
                for (String qual : quals) {
                    String val = vals[v < vals.length ? v : vals.length - 1];
                    long t = ts[v < ts.length ? v : ts.length - 1];
                    System.out.println("Adding: " + row + " " + fam + " " + qual +
                            " " + t + " " + val);
                    put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t,
                            Bytes.toBytes(val));
                    v++;
                }
            }
            tbl.put(put);
        }
        tbl.close();
    }

    public void dump(String table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        dump(TableName.valueOf(table), rows, fams, quals);
    }

    public void dump(TableName table, String[] rows, String[] fams, String[] quals)
            throws IOException {
        Table tbl = connection.getTable(table);
        List<Get> gets = new ArrayList<Get>();
        for (String row : rows) {
            Get get = new Get(Bytes.toBytes(row));
            get.setMaxVersions();
            if (fams != null) {
                for (String fam : fams) {
                    for (String qual : quals) {
                        get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                    }
                }
            }
            gets.add(get);
        }
        Result[] results = tbl.get(gets);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell +
                        ", Value: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()));
            }
        }
        tbl.close();
    }

    public void dump(String table) throws IOException {
        dump(TableName.valueOf(table));
    }

    public void dump(TableName table) throws IOException {
        try (
                Table t = connection.getTable(table);
                ResultScanner scanner = t.getScanner(new Scan())
        ) {
            for (Result result : scanner) {
                dumpResult(result);
            }
        }
    }

    public void dumpResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    ", Value: " + Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }
    }

    public Table getTable(String tableName){
        Table table = null;
        if(tablesByName.containsKey(tableName)){
            table = tablesByName.get(tableName);
        }else{
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                tablesByName.put(tableName, table);
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        return table;
    }

    public void printTableRegions(String tableName) throws IOException{
        System.out.println("Printing regions of table: " + tableName);
        TableName tn = TableName.valueOf(tableName);
        RegionLocator locator = connection.getRegionLocator(tn);
        Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();
        for (int n = 0; n < pair.getFirst().length; n++) {
            byte[] sk = pair.getFirst()[n];
            byte[] ek = pair.getSecond()[n];
            System.out.println("[" + (n + 1) + "]" +
                    " start key: " +
                    (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
                    ", end key: " +
                    (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
        }
        locator.close();
    }



}