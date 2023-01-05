package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.java.tuple.Tuple2;

@Data
public class L2PredictedPrice {
  @SerializedName("product_id") public String productId;
  public long timestamp;
  public Tuple2<Double, Double> prices;
  public Tuple2<Double, Double> err;

  public L2PredictedPrice(String productId, long timestamp, double buyPrice,
                       double sellPrice, double buyErr, double sellErr) {
    this.productId = productId;
    this.timestamp = timestamp;
    this.prices = new Tuple2<Double, Double>(buyPrice, sellPrice);
    this.err = new Tuple2<Double, Double>(buyErr, sellErr);


  }
}