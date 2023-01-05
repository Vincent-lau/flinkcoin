package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class L2Message extends BaseMessage {
  @SerializedName("product_id") public String productId;
  public long timestamp;
  public String timeStr;

  public L2Message(String productId, long timestamp, String timeStr) {
    this.productId = productId;
    this.timestamp = timestamp;
  }

  public L2Message(String productId, long timestamp) {
    this.productId = productId;
    this.timestamp = timestamp;
    this.timeStr = Instant.ofEpochMilli(timestamp).toString();
  }

  public L2Message(String productId, String timeStr) {
    this.productId = productId;
    this.timeStr = timeStr;
    this.timestamp = Instant.parse(timeStr).toEpochMilli();
  }
}
