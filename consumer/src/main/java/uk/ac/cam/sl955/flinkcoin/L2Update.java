package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)

public class L2Update extends BaseMessage {
  @SerializedName("product_id") public String productId;
  public String time;
  @SerializedName("changes") public List<List<String>> changesList;

  public L2Update(L2Update l2Update) {
    this.productId = l2Update.getProductId();
    this.time = l2Update.getTime();
    this.changesList = l2Update.getChangesList();
  }
}