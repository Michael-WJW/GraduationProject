package com.michael.controller;

import com.google.protobuf.ServiceException;
import com.michael.endPointCoprocessor.KNNQueryProto;
import com.michael.model.NeighbourPoint;
import com.michael.model.Point;
import com.michael.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import sun.nio.ch.sctp.SctpNet;

import javax.xml.transform.sax.SAXTransformerFactory;
import java.io.IOException;
import java.util.*;
import java.util.zip.DeflaterInputStream;

/**
 * Created by hadoop on 17-4-13.
 */
@Controller
public class KNNQueryController {
    private static Configuration conf = null;
    private static Connection connection;
    private static HBaseAdmin admin;
    private static final byte[] TABLE_NAME = "GlobalDataBulkLoad".getBytes();
    private static final byte[] REGION_TABLE_NAME = "RegionData".getBytes();
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] GEOHASHSTR = "geohashstr".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private static final Path JARPATH = new Path("hdfs:///HBaseSeondIndexServer19.jar");
    static {
        conf = HBaseConfiguration.create();
        conf.setInt("hbase.rpc.timeout",180000);
        conf.setInt("hbase.client.scanner.timeout.period",180000);
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    protected static Connection getConnection() {
        return KNNQueryController.connection;
    }
    /*
        type:1 无索引GeoHashKNN查询(堆排序数量为geohash过滤后的数据)　2　无索引全表扫描KNN查询(EndPoint实现,堆排序的数量为全表)　
        3　全局索引　4　局部索引
        k　值需小于10000
        latitude :-90~90,longitude:-180~180
        distance:200km以内
     */
    @RequestMapping(value = "/controll/KNNQuery", method = RequestMethod.POST)
    public @ResponseBody Map<String, Object> KNNQuery(double latitude, double longitude,
                         double distance, int k, int type) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        GeoHash geoHashInitial = GeoHash.withDistance(latitude, longitude, distance);
        if (geoHashInitial == null) {
            map.put("result", "查询失败,查询范围需在200km以内！");
            return map;
        }
        GeoHash[] geoHashNeighbours = geoHashInitial.getAdjacent();
        Deque<NeighbourPoint> deque = new ArrayDeque<NeighbourPoint>();
        int i = 0;
        deque.offer(new NeighbourPoint(geoHashInitial, geoHashInitial.changeRestBitsTo1(), i++, 1));
        for (GeoHash geoHash : geoHashNeighbours) {
            deque.offer(new NeighbourPoint(geoHash, geoHash.changeRestBitsTo1(), i++, 1));
        }
        long startTime = 0L, finishTime = 0L;
        List<Point> list = null;
        if (type == 1) {//无索引GeoHashKNN查询
            startTime = System.currentTimeMillis();
            list = KNNWithoutIndex(deque, k);
        } else if (type == 2) {//无索引EndPoint全表扫描KNN查询
            setEndPointCoprocessor();
            startTime = System.currentTimeMillis();
            list = KNNEndPointWithoutIndex(latitude, longitude, k);
        } else if (type == 3) {//全局索引
            setGlobalIndexCoprocessor();
            startTime = System.currentTimeMillis();
            list = KNNWithGlobalIndex(deque, k);
        } else if (type == 4) {//局部索引
            setRegionIndexCoprocessor();
            startTime = System.currentTimeMillis();
            list = KNNWithRegionIndex(deque, k);
        } else {
            map.put("result", "查询失败!");
            return map;
        }
        if (list.size() > k) {
            list = selectNearestKPoints(longitude, latitude, list, k);
        }
        finishTime = System.currentTimeMillis();
        map.put("time", finishTime - startTime + "ms");
        map.put("points", list);
        map.put("result", "查询成功!");
        return map;
    }
    private List<Point> KNNEndPointWithoutIndex(double startLatitude, double startLongitude, int k) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);
        final KNNQueryProto.KNNQueryRequest request = KNNQueryProto.KNNQueryRequest.newBuilder().setK(k)
                .setLatitude(startLatitude).setLongitude(startLongitude).build();
        List<Point> resultList = new ArrayList<Point>();
        try {
            Map<byte[], KNNQueryProto.KNNQueryResponse> map = table.coprocessorService(KNNQueryProto.KNNQueryService.class, null, null,
                    new Batch.Call<KNNQueryProto.KNNQueryService, KNNQueryProto.KNNQueryResponse>() {
                        public KNNQueryProto.KNNQueryResponse call(KNNQueryProto.KNNQueryService aggregate) throws IOException {
                            BlockingRpcCallback<KNNQueryProto.KNNQueryResponse> rpcCallback = new BlockingRpcCallback();
                            aggregate.getKNNQuery(null, request, rpcCallback);
                            KNNQueryProto.KNNQueryResponse response = rpcCallback.get();
                            return response;
                        }
                    });
            for (KNNQueryProto.KNNQueryResponse response : map.values()) {
                List<Double> latList = response.getLatitudeList();
                List<Double> longiList = response.getLongitudeList();
                for (int i = 0; i < latList.size(); i ++) {
                    resultList.add(new Point(latList.get(i), longiList.get(i)));
                }
            }
        }  catch (ServiceException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resultList;
    }
    private List<Point> KNNWithoutIndex(Deque<NeighbourPoint> deque, int k) throws IOException {
//        HTableInterface table = connection.getTable(TABLE_NAME);
        //纬度:35.9　经度:48.9
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        List<Point> list = new ArrayList<Point>(k);
        int level = 1;
        while (!deque.isEmpty() && deque.peek().level == level) {
            putNewNeighbourToDeque(deque);
            NeighbourPoint np = deque.poll();
            byte[] startRow = np.geoHashStart.toBase32().getBytes();
            byte[] endRow = np.geoHashEnd.toBase32().getBytes();
            System.out.println("startROw: " + " " + new String(startRow) + " endRow: " + new String(endRow));
            Filter filter1 = new SingleColumnValueFilter(
                    FAMILY_NAME, GEOHASHSTR, CompareFilter.CompareOp.GREATER_OR_EQUAL, startRow);
            Filter filter2 = new SingleColumnValueFilter(
                    FAMILY_NAME, GEOHASHSTR, CompareFilter.CompareOp.LESS_OR_EQUAL, endRow);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(filter1);
            filterList.addFilter(filter2);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                double latitudeD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LATITUDE));
                double longtideD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LONGITUDE));
//                System.out.println("latitude: " + " " + latitudeD + " " + "longitude: " + longtideD);
                Point point = new Point(latitudeD, longtideD);
                list.add(point);
            }
            rs.close();
            if (!deque.isEmpty() && deque.peek().level != level) {
                level ++;
                if (list.size() >= k || level >= 2) {
                    System.out.println("first level: " + deque.peek().level);
                    break;
                }
            }
        }
        table.close();
        System.out.println("size: " + list.size());
        return list;
    }
    private List<Point> KNNWithIndex(Deque<NeighbourPoint> deque, int k, String range) throws IOException {
        byte[] actualTableName = null;
        if (range.equals("Global")) {
            actualTableName = TABLE_NAME;
        } else if (range.equals("Region")) {
            actualTableName = REGION_TABLE_NAME;
        }
        Table table = connection.getTable(TableName.valueOf(actualTableName));
        List<Point> list = new ArrayList<Point>(k);
        int level = 1;
        while (!deque.isEmpty() && deque.peek().level == level) {
            putNewNeighbourToDeque(deque);
            NeighbourPoint np = deque.poll();
            byte[] startRow = np.geoHashStart.toBase32().getBytes();
            byte[] endRow = np.geoHashEnd.toBase32().getBytes();
            System.out.println("startRow: " + " " + new String(startRow) + " endRow: " + new String(endRow));
            Scan scan = new Scan();
            scan.setAttribute("KNNQueryScan", "true".getBytes());
            scan.setAttribute("startRow", startRow);
            scan.setAttribute("endRow", endRow);
            if (range.equals("Global")) {
                scan.setAttribute("uuid", GenUUID.getUUID().getBytes());
            }
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                System.out.println(new String(result.getRow()));
                double latitudeD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LATITUDE));
                double longtideD = CreateGlobalTable.bytes2Double(result.getValue(FAMILY_NAME, LONGITUDE));
//                System.out.println("latitude: " + " " + latitudeD + " " + "longitude: " + longtideD);
                Point point = new Point(latitudeD, longtideD);
                list.add(point);
            }
            rs.close();
            if (!deque.isEmpty() && deque.peek().level != level) {
                level ++;
                if (list.size() >= k || level >= 2) {
                    System.out.println("first level: " + deque.peek().level);
                    break;
                }
            }
        }
        table.close();
        System.out.println("size: " + list.size());
        return list;
    }
    private List<Point> KNNWithGlobalIndex(Deque<NeighbourPoint> deque, int k) throws IOException {
        return KNNWithIndex(deque, k, "Global");
    }
    private List<Point> KNNWithRegionIndex(Deque<NeighbourPoint> deque, int k) throws IOException {
        return KNNWithIndex(deque, k, "Region");
    }
    private void putNewNeighbourToDeque(Deque<NeighbourPoint> deque) {
        NeighbourPoint first = deque.peek();
        GeoHash firstGeoHash = first.geoHashStart;
        int level = first.level + 1;
        GeoHash north = firstGeoHash.getNorthernNeighbour();
        GeoHash east = firstGeoHash.getEasternNeighbour();
        GeoHash south = firstGeoHash.getSouthernNeighbour();
        GeoHash west = firstGeoHash.getWesternNeighbour();
        if (first.direction == 1) {//北
            if (north != null) {
                NeighbourPoint npNorth = new NeighbourPoint(north, north.changeRestBitsTo1(), 1, level);
                deque.offer(npNorth);
            }
        } else if (first.direction == 2) {//东北
            if (north != null) {
                NeighbourPoint npNorth = new NeighbourPoint(north, north.changeRestBitsTo1(), 1, level);
                deque.offer(npNorth);
            }
            if (east != null) {
                NeighbourPoint npEast = new NeighbourPoint(east, east.changeRestBitsTo1(), 3, level);
                deque.offer(npEast);
            }
            if (north != null && east != null) {
                GeoHash northEast = north.getEasternNeighbour();
                NeighbourPoint npNorthEast = new NeighbourPoint(northEast, northEast.changeRestBitsTo1(), 2, level);
                deque.offer(npNorthEast);
            }
        } else if (first.direction == 3) {//东
            if (east != null) {
                NeighbourPoint npEast = new NeighbourPoint(east, east.changeRestBitsTo1(), 3, level);
                deque.offer(npEast);
            }
        } else if (first.direction == 4) {//东南
            if (east != null) {
                NeighbourPoint npEast = new NeighbourPoint(east, east.changeRestBitsTo1(), 3, level);
                deque.offer(npEast);
            }
            if (south != null) {
                NeighbourPoint npSouth = new NeighbourPoint(south, south.changeRestBitsTo1(), 5, level);
                deque.offer(npSouth);
            }
            if (east != null && south != null) {
                GeoHash southEast = east.getSouthernNeighbour();
                NeighbourPoint npSouthEast = new NeighbourPoint(southEast, southEast.changeRestBitsTo1(), 4, level);
                deque.offer(npSouthEast);
            }
        } else if (first.direction == 5) {//南
            if (south != null) {
                NeighbourPoint npSouth = new NeighbourPoint(south, south.changeRestBitsTo1(), 5, level);
                deque.offer(npSouth);
            }
        } else if (first.direction == 6) {//西南
            if (south != null) {
                NeighbourPoint npSouth = new NeighbourPoint(south, south.changeRestBitsTo1(), 5, level);
                deque.offer(npSouth);
            }
            if (west != null) {
                NeighbourPoint npWest = new NeighbourPoint(west, west.changeRestBitsTo1(), 7, level);
                deque.offer(npWest);
            }
            if (west != null && south != null) {
                GeoHash southWest = west.getSouthernNeighbour();
                NeighbourPoint npSouthWest = new NeighbourPoint(southWest, southWest.changeRestBitsTo1(), 6, level);
                deque.offer(npSouthWest);
            }
        } else if (first.direction == 7) {//西
            if (west != null) {
                NeighbourPoint npWest = new NeighbourPoint(west, west.changeRestBitsTo1(), 7, level);
                deque.offer(npWest);
            }
        } else if (first.direction == 8) {//西北
            if (west != null) {
                NeighbourPoint npWest = new NeighbourPoint(west, west.changeRestBitsTo1(), 7, level);
                deque.offer(npWest);
            }
            if (north != null) {
                NeighbourPoint npNorth = new NeighbourPoint(north, north.changeRestBitsTo1(), 1, level);
                deque.offer(npNorth);
            }
            if (west != null && north != null){
                GeoHash northWest = west.getNorthernNeighbour();
                NeighbourPoint npNorthWest = new NeighbourPoint(northWest, northWest.changeRestBitsTo1(), 8, level);
                deque.offer(npNorthWest);
            }
        }
    }
    private List<Point> selectNearestKPoints(double longitude, double latitude, List<Point> initList, int k) {
        List<DistanceLocation> distanceList = new ArrayList<DistanceLocation>(initList.size());
        for (int i = 0; i < initList.size(); i ++) {
            double distance = PointDistance.LantitudeLongitudeDist(longitude, latitude,
                    initList.get(i).getLongitude(), initList.get(i).getLatitude());
            distanceList.add(new DistanceLocation(distance, i));
        }
        try {
            distanceList = HeapSort.sort(distanceList, k);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<Point> finalList = new ArrayList<Point>(k);
        for (int i = 0; i < k; i ++) {
            finalList.add(initList.get(distanceList.get(i).localtion));
        }
        return finalList;
    }
    private void setEndPointCoprocessor() throws IOException {
        String className = "com.michael.endPointCoprocessor.KNNQueryEndPoint";
        setCoprocessor(className, false);
    }
    private void setGlobalIndexCoprocessor() throws IOException {
        String className = "com.michael.coprocessor.GlobalIndex";
        setCoprocessor(className, false);
    }
    private void setRegionIndexCoprocessor() throws IOException {
        String className = "com.michael.coprocessor.RegionIndex";
        setCoprocessor(className, true);
    }
    public static void setCoprocessor(String className, boolean ifRegion) throws IOException {
        byte[] actualTableName = null;
        if (ifRegion) {
            actualTableName = REGION_TABLE_NAME;
        } else {
            actualTableName = TABLE_NAME;
        }
        TableName tableName = TableName.valueOf(actualTableName);
        Table table = connection.getTable(tableName);
        HTableDescriptor descriptor = table.getTableDescriptor();
        admin.disableTable(actualTableName);
        List<String> coprocessors = descriptor.getCoprocessors();
        for (String coprocessor : coprocessors) {
            descriptor.removeCoprocessor(coprocessor);
        }
        descriptor.addCoprocessor(className, JARPATH, 1001, null);
        admin.modifyTable(actualTableName, descriptor);
        admin.enableTable(actualTableName);
    }
    static class DistanceLocation implements Comparable<DistanceLocation> {
        double distance;
        int localtion;
        protected DistanceLocation(double distance, int localtion) {
            this.distance = distance;
            this.localtion = localtion;
        }
        public int compareTo(DistanceLocation o) {
            if (this.distance > o.distance) {
                return 1;
            } else if (this.distance == o.distance) {
                return 0;
            } else {
                return -1;
            }
        }

        @Override
        public String toString() {
            return "DistanceLocation{" +
                    "distance=" + distance +
                    ", localtion=" + localtion +
                    '}';
        }
    }


}
