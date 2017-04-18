package com.michael.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hadoop on 17-4-8.
 */
public class RegionIndex extends BaseRegionObserver {
    private static final byte[] TABLE_NAME = "RegionData".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] QUAFILER_NAME = "geohashstr".getBytes();
    private static final byte[] INDEX_FAMILY_NAME = "index".getBytes();
    private static final byte[] INDEX_QUALIFIER_NAME = "qualifier".getBytes();
//    private static final Log logger = LogFactory.getLog(RegionIndex.class);
    private static ConcurrentHashMap<RegionScanner, Scan> map = new ConcurrentHashMap<RegionScanner, Scan>();
    //对同一个region上的操作在此shouldPassMap上可能会有竞争
    private static ConcurrentHashMap<RegionScanner, Boolean> shouldPassMap = new ConcurrentHashMap<RegionScanner, Boolean>();
    private static Configuration conf;
    private static HConnection connection;
    private static HTableInterface table;
    public RegionIndex() {
        conf = HBaseConfiguration.create();
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(TABLE_NAME);
        } catch (IOException e) {

            e.printStackTrace();
        }
    }
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (put.getAttribute("index_put") == null) {
            String rowKey = new String (put.getRow());
            List<Cell> cellList = put.get(FAMILY_NAME, QUAFILER_NAME);
            String geohashstr = new String (cellList.get(0).getValue());
            String startKey = rowKey.split("\\|")[0];
            String index_rowKey = startKey + "-" + geohashstr + "-" + rowKey;
            Put index_put = new Put(index_rowKey.getBytes());
            index_put.addColumn(INDEX_FAMILY_NAME, INDEX_QUALIFIER_NAME, null);
            index_put.setAttribute("index_put", INDEX_FAMILY_NAME);
//            e.getEnvironment().getRegion().put(index_put);
//            HTableInterface table = connection.getTable(TABLE_NAME);
            table.put(index_put);//table是不是线程安全的呢？
//            table.close();
//            table.close();
        }
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
//        logger.info("postScannerOpen!" + " regionScanner: " + s);
        map.put(s, scan);
        return super.postScannerOpen(e, scan, s);
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
//        logger.info("preScannerNext");
//        logger.info("");
        Scan scan = map.get(s);
        if (scan != null && (scan.getAttribute("KNNQueryScan") != null || scan.getAttribute("RangeQueryScan") != null)) {
            HRegion region = e.getEnvironment().getRegion();
            byte[] startKey = null;
            byte[] endKey = null;
            if (scan.getAttribute("startRow") != null && scan.getAttribute("endRow") != null) {
                String startKeyAttr = new String (scan.getAttribute("startRow"));
                String endKeyAttr = new String(scan.getAttribute("endRow"));
                List<Cell> startRow = new ArrayList<Cell>();
                s.next(startRow);
                String regionStartKey = new String(startRow.get(startRow.size() - 1).getRow());
                String regionPrefix = regionStartKey.split("-")[0];
                startKey =  (regionPrefix + "-" + startKeyAttr).getBytes();
                endKey = (regionPrefix + "-" + endKeyAttr + "@").getBytes();
//                logger.info("regionStartKey: " + regionStartKey + " " +
//                    new String(startKey) + " " + new String(endKey));
            }
            if (startKey != null && endKey != null) {
//            logger.info("comeIn!");
                Scan regionScan = new Scan();
                regionScan.setStartRow(startKey);
                regionScan.setStopRow(endKey);
                RegionScanner rs = region.getScanner(regionScan);
                List<Cell> list = new ArrayList<Cell>();
                String index_rowKey = null;
                String finalRowKey = null;
                while (rs.nextRaw(list)) {
                    index_rowKey = new String(list.get(list.size() - 1).getRow());
                    byte[] rowKey = (index_rowKey.split("-")[2]).getBytes();
//                    logger.info("size: " + list.size() + "  rowkey: " + new String(rowKey));
                    Get get = new Get(rowKey);
                    Result result = region.get(get);
//                    for (Cell cell : result.listCells()) {
//                        logger.info("row: " + new String(cell.getRow()) +" family: " + new String(cell.getFamily()) + " qualifier: " + new String(cell.getQualifier())
//                            + " value: " + new String(cell.getValue()));
//                    }
//                    logger.info("");
                    results.add(result);
                    list.clear();
                }
                if (list.size() > 0) {
                    finalRowKey = new String(list.get(list.size() - 1).getRow());
                    byte[] rowKey = (finalRowKey.split("-")[2]).getBytes();
//                    logger.info("finalRowKey: " + new String(rowKey));
                    Get get = new Get(rowKey);
                    Result result = region.get(get);
                    results.add(result);
                    list.clear();
                }
                rs.close();
                if (finalRowKey != null) {
                    scan.setAttribute("startRow", (finalRowKey.split("-")[1] + "@").getBytes());
                    map.put((RegionScanner) s, scan);
                } else if (index_rowKey != null) {
                    scan.setAttribute("startRow", (index_rowKey.split("-")[1] + "@").getBytes());
                    map.put((RegionScanner) s, scan);
                }
                shouldPassMap.put((RegionScanner) s, true);
            }
        }
        if (shouldPassMap.get(s) != null && shouldPassMap.get(s)) {
            hasMore = true;
            e.bypass();
        }
        return super.preScannerNext(e, s, results, limit, hasMore);
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
//        logger.info("preScannerClose");
        map.remove(s);
        shouldPassMap.remove(s);
        super.preScannerClose(e, s);
    }
}
