package com.michael.endPointCoprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.michael.utils.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ScanPerformanceEvaluation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-15.
 */
public class KNNQueryEndPoint extends KNNQueryProto.KNNQueryService implements Coprocessor, CoprocessorService {
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private RegionCoprocessorEnvironment env;
//    private static final Log logger = LogFactory.getLog(KNNQueryEndPoint.class);
    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
//            logger.info("load on a table region");
            this.env = (RegionCoprocessorEnvironment)coprocessorEnvironment;
        } else {
            throw new IOException("coprocessorEnvironment must instanceof RegionCoprocessorEnvironment!");
//            logger.info("didn't loaded on a table region! className: " + coprocessorEnvironment.getClass().getName());
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getKNNQuery(RpcController controller, KNNQueryProto.KNNQueryRequest request, RpcCallback<KNNQueryProto.KNNQueryResponse> done) {
        double startLatitude = request.getLatitude();
        double startLongitude = request.getLongitude();
        int k = request.getK();
//        logger.info("startLatitude: " + startLatitude + " startLongitude: " + startLongitude + " k: " + k);
        Scan scan = new Scan();
        scan.addColumn(FAMILY_NAME, LATITUDE);
        scan.addColumn(FAMILY_NAME, LONGITUDE);
        KNNQueryProto.KNNQueryResponse response = null;
        InternalScanner scanner = null;
        try {
            List<Point> resultPoints = new ArrayList<>(k);
            List<DistanceLocation> distanceLocations = new ArrayList<>();
            scanner = env.getRegion().getScanner(scan);
//            logger.info("当前scanner: " + scanner);
            List<Cell> results = new ArrayList();
            boolean hasMore = false;
            long i = 0;
//            long count = 0;
            do {
                hasMore = scanner.next(results);
//                if (count % 10000 == 0) {
//                    logger.info("当前scanner: " + scanner +" size: " + results.size() + " count: " + count);
//                    if (results.size() > 1) {
//                        logger.info("当前scanner: " + scanner + " 当前经纬度：　" + new String(results.get(0).getValue()) + " " + new String(results.get(1).getValue()));
//                    }
//                }
//                count ++;
                double latitude = 0, longitude = 0;
                for (Cell cell : results) {
                    if (new String(cell.getQualifier()).equals("latitude")) {
                        latitude = DoubleBytes.bytes2Double(cell.getValue());
//                        latitude = Double.parseDouble(new String(cell.getValue()));
                    }
                    if (new String(cell.getQualifier()).equals("longitude")) {
                        longitude = DoubleBytes.bytes2Double(cell.getValue());
//                        longitude = Double.parseDouble(new String(cell.getValue()));
                    }
                }
                results.clear();
                double distance = PointDistance.LantitudeLongitudeDist(startLongitude, startLatitude, longitude, latitude);
                if (i < k) {
                    distanceLocations.add(new DistanceLocation(distance, i));
                    resultPoints.add(new Point(latitude, longitude));
                    i ++;
                    if (i == k) {
                        HeapSort.initialHeap(distanceLocations);
                    }
                } else {
                    if (distance < distanceLocations.get(0).getDistance()) {
                        distanceLocations.get(0).setDistance(distance);
                        resultPoints.set((int)distanceLocations.get(0).getLocaltion(), new Point(latitude, longitude));
                        HeapSort.heapify(distanceLocations, k, 0);
                    }
                }
            } while (hasMore);
//            logger.info("scanner: " + scanner + " 当前region 读取完毕");
            KNNQueryProto.KNNQueryResponse.Builder builder = KNNQueryProto.KNNQueryResponse.newBuilder();
            for (Point point : resultPoints) {
                builder.addLatitude(point.getLatitude());
                builder.addLongitude(point.getLongitude());
            }
            response = builder.build();
//            logger.info("scanner: " + scanner + " response 构造完毕");
        } catch (IOException ioe) {
//            logger.info("scanner: " + scanner + " exception: " + ioe);
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);
    }
}
