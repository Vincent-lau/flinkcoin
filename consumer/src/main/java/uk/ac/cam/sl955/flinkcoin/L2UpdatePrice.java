package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.sql.Timestamp;
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

  public L2UpdatePrice(L2UpdateMessage l2UpdateMessage, double buyPrice, double sellPrice) {
    this.productId = l2UpdateMessage.getProductId();
    this.timeStr = l2UpdateMessage.getTime();
    Timestamp ts = Timestamp.valueOf(l2UpdateMessage.getTime().replace("T", " ").replace("Z", ""));
    this.timeLong = ts.getTime();
    this.buyPrice = buyPrice;
    this.sellPrice = sellPrice;

  }
}