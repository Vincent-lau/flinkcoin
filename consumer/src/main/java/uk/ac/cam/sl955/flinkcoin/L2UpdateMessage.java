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

class L2Update extends BaseMessage {
  @SerializedName("product_id") public String productId;
  public String time;
  @SerializedName("changes") public List<List<String>> changesList;

  public L2Update(L2Update l2Update) {
    this.productId = l2Update.getProductId();
    this.time = l2Update.getTime();
    this.changesList = l2Update.getChangesList();
  }

  public List<List<String>> getChangesList() { return changesList; }
}

@Data
public class L2UpdateMessage extends L2Update {

  public List<Change> changes;

  public L2UpdateMessage(L2Update l2Update) {
    super(l2Update);

    if (l2Update.getChangesList() == null) {
      this.changes = new ArrayList<Change>();
    } else {
      this.changes =
          l2Update.getChangesList()
              .stream()
              .map(change
                   -> new Change(change.get(0), change.get(1), change.get(2)))
              .collect(Collectors.toList());
    }
  }

  public String toString() {
    return String.format("L2UpdateMessage: %s %s %s %s", productId, time,
                         changesList, changes);
  }
}

enum Side { @SerializedName("buy") BUY, @SerializedName("sell") SELL }

@Data
class Change {
  public Side side;
  public double price;
  public double size;

  public Change(String side, String price, String size) {
    this.side = Side.valueOf(side.toUpperCase());
    this.price = Double.parseDouble(price);
    this.size = Double.parseDouble(size);
  }
}
