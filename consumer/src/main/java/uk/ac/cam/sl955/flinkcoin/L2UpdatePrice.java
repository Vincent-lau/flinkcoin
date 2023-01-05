package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public class L2UpdatePrice {
  @SerializedName("product_id") public String productId;
  public long timeLong;
  public String timeStr;
  public double buyPrice;
  public double sellPrice;
  private final static double EPS = 1e-5;

  public L2UpdatePrice(L2UpdateMessage l2UpdateMessage, double buyPrice,
                       double sellPrice) {
    this.productId = l2UpdateMessage.getProductId();
    this.timeStr = l2UpdateMessage.getTime();
    this.timeLong = Instant.parse(timeStr).toEpochMilli();
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;
  }

  public L2UpdatePrice(String productId, long timeLong, String timeStr,
                       double buyPrice, double sellPrice) {
    this.productId = productId;
    this.timeLong = timeLong;
    this.timeStr = timeStr;
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;
  }

  public L2UpdatePrice(String productId, long timeLong, double buyPrice,
                       double sellPrice) {
    this.productId = productId;
    this.timeLong = timeLong;
    this.timeStr = Instant.ofEpochMilli(timeLong).toString();
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;
  }

  public boolean validPrice() {
    return buyPrice > EPS && sellPrice > EPS;
  }
}