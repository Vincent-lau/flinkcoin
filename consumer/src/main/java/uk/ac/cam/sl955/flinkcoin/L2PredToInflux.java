package uk.ac.cam.sl955.flinkcoin;

import java.util.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;

public class L2PredToInflux
    extends RichMapFunction<L2PredictedPrice, InfluxDBPoint> {

  private PriceType priceType;

  public L2PredToInflux() { this.priceType = PriceType.ACTUAL; }
  public L2PredToInflux(PriceType pt) { this.priceType = pt; }

  @Override
  public InfluxDBPoint map(L2PredictedPrice prediction) throws Exception {
    String measurement = prediction.getProductId();
    long timestamp = prediction.getTimestamp();

    HashMap<String, Object> fields = new HashMap<>();
    HashMap<String, String> tags = new HashMap<>();
    fields.put("buy", prediction.getPrices().f0);
    fields.put("sell", prediction.getPrices().f1);
    fields.put("buyErr", prediction.getErr().f0);
    fields.put("sellErr", prediction.getErr().f1);

    tags.put("price type", priceType.toString());

    return new InfluxDBPoint(measurement, timestamp, tags, fields);
  }
}


