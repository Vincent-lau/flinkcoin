package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.annotations.SerializedName;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.java.tuple.Tuple2;

@Data
public class L2AutoCorr extends L2Message {

  public Map<Integer, Double> autoCorr;

  public L2AutoCorr(String productId, long timestamp,
                          Map<Integer, Double> autoCorr) {
    super(productId, timestamp);
    this.autoCorr = autoCorr;
  }
}