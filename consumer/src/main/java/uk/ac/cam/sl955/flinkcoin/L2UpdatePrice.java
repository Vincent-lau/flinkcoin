package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class L2UpdatePrice {

  @SerializedName("product_id") public String productId;
  @SerializedName("time_stamp") public long time_stamp;
  @SerializedName("buy_price") public double buyPrice;
  @SerializedName("sell_price") public double sellPrice;

  public L2UpdatePrice() {}

  public L2UpdatePrice(String productId, long time_stamp, double buyPrice,
                       double sellPrice) {
    this.productId = productId;
    this.time_stamp = time_stamp;
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;
  }

  public String getTimeStr() {
    return Instant.ofEpochMilli(getTimestamp()).toString();
  }

  public boolean validPrice() {
    final double EPS = 1e-5;
    return buyPrice > EPS && sellPrice > EPS;
  }

  public long getTimestamp() {
    return time_stamp;
  }
}