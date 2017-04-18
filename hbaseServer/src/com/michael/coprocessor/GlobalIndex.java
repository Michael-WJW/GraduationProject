package com.michael.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.Arrays;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hadoop on 17-4-8.
 */
public class GlobalIndex extends BaseRegionObserver {
    private static final byte[]  INDEX_TABLE_NAME = "GlobalDataIndex".getBytes();
    private static final byte[]  INDEX_TABLE_FAMILY_NAME = "null".getBytes();
    private static final byte[]  INDEX_TABLE_QUALIFIERNAME = "null".getBytes();

    private static final byte[] INITIAL_TABLE_NAME = "GlobalData".getBytes();
    private static final byte[] INITIAL_TABLE_FAMILYNAME = "info".getBytes();
    private static final byte[] INITIAL_TABLE_QUALIFIERNAME = "geohashstr".getBytes();

    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();

    //不同region在此map上可能存在竞争
    private static ConcurrentHashMap<String , RegionScanner> regionMap = new ConcurrentHashMap<String , RegionScanner>();

    //对同一个region上的操作在此map上可能会有竞争
    private static ConcurrentHashMap<RegionScanner, Scan> map = new ConcurrentHashMap<RegionScanner, Scan>();

    //对同一个region上的操作在此shouldPassMap上可能会有竞争
    private static ConcurrentHashMap<RegionScanner, Boolean> shouldPassMap = new ConcurrentHashMap<RegionScanner, Boolean>();
    private Configuration conf;
    private HConnection connection;//注意代码中connection连接一直未关闭
    private HBaseAdmin admin;
    private HTableInterface index_table;
    public GlobalIndex() throws IOException {
        conf = HBaseConfiguration.create();
        connection = HConnectionManager.createConnection(conf);
        admin = new HBaseAdmin(conf);
        index_table = connection.getTable(INDEX_TABLE_NAME);
    }
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String initial_table_rowkey = new String(put.getRow());
        List<Cell> list = put.get(INITIAL_TABLE_FAMILYNAME, INITIAL_TABLE_QUALIFIERNAME);
        if (put.getAttribute("clearRegionMap") == null && list != null && list.size() > 0) {
            String initial_table_geohashstr = new String(list.get(0).getValue());//先考虑只有一个版本的情况
            String index_table_rowkey = initial_table_geohashstr + "_" + initial_table_rowkey;
            Put index_put = new Put(index_table_rowkey.getBytes());
            index_put.addColumn(INDEX_TABLE_FAMILY_NAME, INDEX_TABLE_QUALIFIERNAME, null);
            index_table.put(index_put);
//            index_table.close();
        }
        if (put.getAttribute("clearRegionMap") != null) {
            regionMap.clear();
            e.bypass();
        }
    }


    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        map.put(s, scan);
        return super.postScannerOpen(e, scan, s);
    }
    //preScannerNext为何会被多次调用？？？？？
    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        Scan scan = map.get(s);
        map.remove(s);
        if (scan != null && (scan.getAttribute("KNNQueryScan") != null || scan.getAttribute("RangeQueryScan") != null)) {
            synchronized (GlobalIndex.class) {
                if (regionMap.get(new String (scan.getAttribute("uuid"))) == null) {
                    regionMap.put(new String (scan.getAttribute("uuid")), (RegionScanner) s);
                }
            }
            //待解决:regionMap中何时将键值对移除呢？？?
            if (regionMap.get(new String (scan.getAttribute("uuid"))) == s) {
                List<String> list = new ArrayList<String>();
                HTableInterface index_table = connection.getTable(INDEX_TABLE_NAME);
                //接下来进行对索引表的rowkey 的scan操作，返回符合条件的 rowkey
                byte[] startRow = scan.getAttribute("startRow");
                byte[] endRow = (new String(scan.getAttribute("endRow")) + "`").getBytes();
                Scan index_scan = new Scan();
                index_scan.setStartRow(startRow);
                index_scan.setStopRow(endRow);
                ResultScanner rs = index_table.getScanner(index_scan);
                for (Result result : rs) {
                    String index_rowKey = new String(result.getRow());
                    String rowKey = index_rowKey.split("_")[1];
                    list.add(rowKey);
                }
                index_table.close();
                List<Get> getList = new ArrayList<Get>();
                HTableInterface table = connection.getTable(INITIAL_TABLE_NAME);
                for (int i = 0; i < list.size(); i++) {
                    Get get = new Get(list.get(i).getBytes());
                    get.addColumn(INITIAL_TABLE_FAMILYNAME, LATITUDE);
                    get.addColumn(INITIAL_TABLE_FAMILYNAME, LONGITUDE);
                    getList.add(get);
                }
                //批量提交get操作
                Result[] final_result = table.get(getList);
                results.addAll(Arrays.asList(final_result));
                table.close();
            }
            shouldPassMap.put((RegionScanner) s, true);
        }
        if (shouldPassMap.get(s) != null && shouldPassMap.get(s)) {
            hasMore = true;
            e.bypass();
        }
        return super.preScannerNext(e, s, results, limit, hasMore);
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
        shouldPassMap.remove(s);
        super.preScannerClose(e, s);
    }
}
