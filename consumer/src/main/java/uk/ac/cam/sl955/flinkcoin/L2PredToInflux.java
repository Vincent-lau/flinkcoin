package uk.ac.cam.sl955.flinkcoin;

import java.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

public class L2PredToInflux
    extends RichMapFunction<L2PredictedPrice, InfluxDBPoint> {

  @Override
  public InfluxDBPoint map(L2PredictedPrice prediction) throws Exception {
    String measurement = prediction.getProductId();
    long timestamp = prediction.getTimestamp();

    HashMap<String, Object> fields = new HashMap<>();
    fields.put("buyPrediction", prediction.getPrices().f0);
    fields.put("sellPrediction", prediction.getPrices().f1);
    fields.put("buyErr", prediction.getErr().f0);
    fields.put("sellErr", prediction.getErr().f1);
    return new InfluxDBPoint(measurement, timestamp, new HashMap<>(), fields);
  }
}