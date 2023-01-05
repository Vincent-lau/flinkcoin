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

  public String productId;
  public long timestamp;
  public double buyPrice;
  public double sellPrice;
  private transient final static double EPS = 1e-5;

  public L2UpdatePrice(String productId, long timestamp, double buyPrice,
                       double sellPrice) {
    this.productId = productId;
    this.timestamp = timestamp;
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;
  }

  public String getTimeStr() {
    return Instant.ofEpochMilli(timestamp).toString();
  }

  public boolean validPrice() { return buyPrice > EPS && sellPrice > EPS; }
}