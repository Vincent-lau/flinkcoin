package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@Data
@ToString(callSuper = true)
public class L2UpdateMessage extends L2Message {

  public List<Change> changes;

  public L2UpdateMessage(L2Update l2Update) {
    super(l2Update.getProductId(), l2Update.getTime());

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
