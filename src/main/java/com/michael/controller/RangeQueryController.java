package com.michael.controller;

import com.michael.endPointCoprocessor.RangeQueryProto;
import com.michael.model.GeoHashRange;
import com.michael.model.Point;
import com.michael.utils.CreateGlobalTable;
import com.michael.utils.GenUUID;
import com.michael.utils.GeoHash;
import com.michael.utils.RangeQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.*;

/**
 * Created by hadoop on 17-4-17.
 */
@Controller
public class RangeQueryController {
    private static Configuration conf = null;
    private static Connection connection;
    private static final byte[] TABLE_NAME = "GlobalDataBulkLoad".getBytes();
    private static final byte[] REGION_TABLE_NAME = "RegionData".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    static {
        conf = HBaseConfiguration.create();
        conf.setInt("hbase.rpc.timeout",180000);
        conf.setInt("hbase.client.scanner.timeout.period",180000);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //起始经纬度：40.8, 30.8 结束经纬度: 60.9, 60.5
    //type: 1 无索引EndPoint 2 全局索引 3 局部索引
    @RequestMapping(value = "/controll/RangeQuery", method = RequestMethod.POST)
    public @ResponseBody Map<String, Object> rangeQuery(double startLatitude, double startLongitude,
                                 double endLatitude, double endLongitude, int type) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        List<Point> list = null;
        long startTime = 0L;
        if (type == 1) {//无索引EndPoint
            setEndPointCoprocessor();
            startTime = System.currentTimeMillis();
            list = RangeQueryEndPointWithoutIndex(startLatitude, startLongitude, endLatitude, endLongitude);
        } else if (type == 2) {//全局索引
            setGlobalIndexCoprocessor();
            startTime = System.currentTimeMillis();
            list = rangeQueryWithGlobalIndex(startLatitude, startLongitude, endLatitude, endLongitude);
        } else if (type == 3) {//局部索引
            setRegionIndexCoprocessor();
            startTime = System.currentTimeMillis();
            list = rangeQueryWithRegionIndex(startLatitude, startLongitude, endLatitude, endLongitude);
        } else {
            map.put("result", "查询失败！");
            return map;
        }
        long finishTime = System.currentTimeMillis();
        map.put("time", finishTime - startTime + "ms");
        map.put("points", list);
        map.put("result", "查询成功!");
        return map;
    }
    private void setGlobalIndexCoprocessor() throws IOException {
        KNNQueryController.setCoprocessor("com.michael.coprocessor.GlobalIndex", false);
    }
    private void setRegionIndexCoprocessor() throws IOException {
        KNNQueryController.setCoprocessor("com.michael.coprocessor.RegionIndex", true);
    }
    private void setEndPointCoprocessor() throws IOException {
        KNNQueryController.setCoprocessor("com.michael.endPointCoprocessor.RangeQueryEndPoint", false);
    }
    private List<Point> rangeQueryWithIndex(double startLatitude, double startLongitude,
                                                  double endLatitude, double endLongitude, String range1) throws IOException {
        byte[] actualTableName = null;
        if (range1.equals("Global")) {
            actualTableName = TABLE_NAME;
        } else if (range1.equals("Region")) {
            actualTableName = REGION_TABLE_NAME;
        }
        List<Point> list = new ArrayList<Point>();
        System.out.println("table name: " + actualTableName);
        TableName tableName = TableName.valueOf(actualTableName);
        Table table = connection.getTable(tableName);
        Deque<GeoHashRange> deque = RangeQuery.query(startLatitude, startLongitude, endLatitude, endLongitude);
//        System.out.println("deque size: " + deque.size());
        for (GeoHashRange range : deque) {
            String leftDown = GeoHash.longToGeoHash(range.leftDown);
            String rightUp = GeoHash.longToGeoHash(range.rightUp);
            Scan scan = new Scan();
            scan.setAttribute("RangeQueryScan", "true".getBytes());
            scan.setAttribute("startRow", leftDown.getBytes());
            scan.setAttribute("endRow", rightUp.getBytes());
            System.out.println("startRow: " + leftDown + " endRow: " + rightUp);

            if (range1.equals("Global")) {
                scan.setAttribute("uuid", GenUUID.getUUID().getBytes());
            }
//            System.out.println("leftDown: " + leftDown + "  " + "rightUp: " + rightUp + " count: " + count);
            ResultScanner rs = table.getScanner(scan);
            int count = 0;
            for (Result result : rs) {
//                System.out.println(result.getValue(FAMILY_NAME,LATITUDE));
//                System.out.println(result.getValue(FAMILY_NAME,LONGITUDE));
//                for (Cell cell : result.listCells()) {
//                    System.out.println("qualifier: " + new String(cell.getQualifier()));
//                    System.out.println("row: " + new String(cell.getRow()));
//                }
                System.out.println("count: " + count ++ + "  " + new String(result.getRow()));
                double latitudeD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LATITUDE));
                double longtideD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LONGITUDE));
                if (startLatitude < latitudeD && latitudeD < endLatitude &&
                        startLongitude < longtideD && longtideD < endLongitude) {
                    Point p = new Point(latitudeD, longtideD);
                    list.add(p);
                }
            }
            rs.close();
        }
//        System.out.println(list.size());
        table.close();
        return list;
    }
    private List<Point> rangeQueryWithGlobalIndex(double startLatitude, double startLongitude,
                                                  double endLatitude, double endLongitude) throws IOException {
        return rangeQueryWithIndex(startLatitude, startLongitude, endLatitude, endLongitude, "Global");
    }
    private List<Point> rangeQueryWithRegionIndex(double startLatitude, double startLongitude,
                                                  double endLatitude, double endLongitude) throws IOException {
        return rangeQueryWithIndex(startLatitude, startLongitude, endLatitude, endLongitude, "Region");
    }
    private List<Point> RangeQueryEndPointWithoutIndex(double startLatitude, double startLongitude,
                                                       double endLatitude, double endLongitude) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);
        final RangeQueryProto.RangeQueryRequest request = RangeQueryProto.RangeQueryRequest.newBuilder()
                .setStartLatitude(startLatitude).setEndLatitude(endLatitude).setStartLongitude(startLongitude)
                    .setEndLongitude(endLongitude).build();
        List<Point> resultList = new ArrayList<Point>();
        try {
            Map<byte[], RangeQueryProto.RangeQueryResponse> map = table.coprocessorService(
                    RangeQueryProto.RangeQueryService.class, null, null,
                    new Batch.Call<RangeQueryProto.RangeQueryService, RangeQueryProto.RangeQueryResponse>() {

                        public RangeQueryProto.RangeQueryResponse call(RangeQueryProto.RangeQueryService aggregate) throws IOException {
                            BlockingRpcCallback<RangeQueryProto.RangeQueryResponse> rpcCallback = new BlockingRpcCallback();
                            aggregate.getRangeQuery(null, request, rpcCallback);
                            RangeQueryProto.RangeQueryResponse response = rpcCallback.get();
                            return response;
                        }
                    }
            );
            for (RangeQueryProto.RangeQueryResponse response : map.values()) {
                List<Double> latList = response.getLatitudeList();
                List<Double> longiList = response.getLongitudeList();
                for (int i = 0; i < latList.size(); i ++) {
                    resultList.add(new Point(latList.get(i), longiList.get(i)));
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return resultList;
    }
}
