package com.endpoint.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import com.endpoint.test.Sum.SumRequest;
import com.endpoint.test.Sum.SumResponse;
import com.endpoint.test.Sum.SumService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class SumEndPoint extends SumService implements Coprocessor,CoprocessorService{

    private RegionCoprocessorEnvironment env;   // 定义环境


    @Override
    public void getSum(RpcController controller, SumRequest request, RpcCallback<SumResponse> done) {
        String family = request.getFamily();
        if (null == family || "".equals(family)) {
            throw new NullPointerException("you need specify the family");
        }
        String columnS = request.getColumns();
        if (null == columnS || "".equals(columnS))
            throw new NullPointerException("you need specify the column");


        Scan scan = new Scan();

            // 设置扫描对象
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnS));


        // 定义变量
        SumResponse response = null;
        InternalScanner scanner = null;

        // 扫描每个region，取值后求和
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            Long sum = 0L;
            do {
                hasMore = scanner.next(results);
                //for (Cell cell : results) {
                   if (results.isEmpty()) {
                        continue;
                    }
                    Cell kv = results.get(0);
                    Long value = 0L;
                    try {
                        value = Long.parseLong(Bytes.toString(CellUtil.cloneValue(kv)));
                    } catch (Exception e) {
                    }

                    sum += value;
                    results.clear();
                    //sum += Long.parseLong(new String(CellUtil.cloneValue(cell)));

                //results.clear();
            } while (hasMore);
            // 设置返回结果
            response = SumResponse.newBuilder().setSum(sum).build();
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    //e.printStackTrace();
                }
            }
        }
        // 将rpc结果返回给客户端
        done.run(response);

    }


    @Override
    public Service getService() {

        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("no load region");
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

}