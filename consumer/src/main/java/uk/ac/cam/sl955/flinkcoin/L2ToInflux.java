package uk.ac.cam.sl955.flinkcoin;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import java.util.*;

public class L2ToInflux extends RichMapFunction<L2UpdatePrice, InfluxDBPoint> {

  @Override
  public InfluxDBPoint map(L2UpdatePrice l2) throws Exception {

    String measurement = l2.getProductId();

    long timestamp = l2.getTimeLong();

    HashMap<String, Object> fields = new HashMap<>();
    fields.put("buy", l2.getBuyPrice());
    fields.put("sell", l2.getSellPrice());

    return new InfluxDBPoint(measurement, timestamp,
                             new HashMap<String, String>(), fields);
  }
}
