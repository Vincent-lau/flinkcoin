package uk.ac.cam.sl955.flinkcoin;

import java.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

public class L2ToInflux extends RichMapFunction<L2UpdatePrice, InfluxDBPoint> {

  @Override
  public InfluxDBPoint map(L2UpdatePrice l2) throws Exception {

    String measurement = l2.getProductId();

    long timestamp = l2.getTimestamp();

    HashMap<String, Object> fields = new HashMap<>();
    HashMap<String, String> tags = new HashMap<>();
    fields.put("buy", l2.getBuyPrice());
    fields.put("sell", l2.getSellPrice());
    tags.put("price type", PriceType.ACTUAL.toString());

    return new InfluxDBPoint(measurement, timestamp, tags, fields);
  }
}

