package uk.ac.cam.sl955.flinkcoin;

import java.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

public class AR2Influx extends RichMapFunction<L2AutoCorr, InfluxDBPoint> {

  @Override
  public InfluxDBPoint map(L2AutoCorr l2AutoCorr) throws Exception {
    String measurement = l2AutoCorr.getProductId();
    long timestamp = l2AutoCorr.getTimestamp();

    Random rand = new Random();
    int randInt = rand.nextInt(3);

    HashMap<String, Object> fields = new HashMap<>();
    HashMap<String, String> tags = new HashMap<>();
    tags.put("lag", Integer.toString(randInt));
    fields.put("buy auto correlation",
               l2AutoCorr.getAutoCorr().get(randInt));
    
    return new InfluxDBPoint(measurement, timestamp, tags, fields);
  }
}