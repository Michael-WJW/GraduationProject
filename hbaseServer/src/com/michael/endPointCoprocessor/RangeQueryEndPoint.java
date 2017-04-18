package com.michael.endPointCoprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.michael.utils.DoubleBytes;
import com.michael.utils.Point;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.security.interfaces.DSAPublicKey;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 17-4-17.
 */
public class RangeQueryEndPoint extends RangeQueryProto.RangeQueryService implements Coprocessor, CoprocessorService {
    private static final byte[] FAMILY_NAME = "info".getBytes();
    private static final byte[] LATITUDE = "latitude".getBytes();
    private static final byte[] LONGITUDE = "longitude".getBytes();
    private RegionCoprocessorEnvironment env;
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
    public void getRangeQuery(RpcController controller, RangeQueryProto.RangeQueryRequest request, RpcCallback<RangeQueryProto.RangeQueryResponse> done) {
        double startLatitude = request.getStartLatitude();
        double startLongitude = request.getStartLongitude();
        double endLatitude = request.getEndLatitude();
        double endLongitude = request.getEndLongitude();
        Scan scan = new Scan();
        scan.addColumn(FAMILY_NAME, LATITUDE);
        scan.addColumn(FAMILY_NAME, LONGITUDE);
        RangeQueryProto.RangeQueryResponse response = null;
        InternalScanner scanner = null;
        try {
            List<Point> resultPoints = new ArrayList<>();
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList();
            boolean hasMore = false;
            do {
                hasMore = scanner.next(results);
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
                if (startLatitude < latitude && latitude < endLatitude &&
                        startLongitude < longitude && longitude < endLongitude) {
                    resultPoints.add(new Point(latitude, longitude));
                }
            } while (hasMore);
            RangeQueryProto.RangeQueryResponse.Builder builder = RangeQueryProto.RangeQueryResponse.newBuilder();
            for (Point point : resultPoints) {
                builder.addLatitude(point.getLatitude());
                builder.addLongitude(point.getLongitude());
            }
            response = builder.build();
        }  catch (IOException ioe) {
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
