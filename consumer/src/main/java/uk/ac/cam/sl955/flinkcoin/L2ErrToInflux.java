package uk.ac.cam.sl955.flinkcoin;

import java.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

public class L2ErrToInflux
    extends RichMapFunction<L2PredictedPrice, InfluxDBPoint> {

  private PriceType errType;

  public L2ErrToInflux(PriceType pt) { this.errType = pt; }

  @Override
  public InfluxDBPoint map(L2PredictedPrice prediction) throws Exception {
    String measurement = prediction.getProductId();
    long timestamp = prediction.getTimestamp();

    HashMap<String, Object> fields = new HashMap<>();
    HashMap<String, String> tags = new HashMap<>();
    fields.put("buy", prediction.getErr().f0);
    fields.put("sell", prediction.getErr().f1);

    tags.put("price type", errType.toString());

    return new InfluxDBPoint(measurement, timestamp, tags, fields);
  }
}

