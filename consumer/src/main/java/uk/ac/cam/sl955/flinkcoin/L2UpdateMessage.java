package uk.ac.cam.sl955.flinkcoin;


import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)

public class L2UpdateMessage extends BaseMessage {
    @SerializedName("product_id")
    public String productId;
    public String time;
    public List<List<String>> changes;
}