package uk.ac.cam.sl955.flinkcoin;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;

public class PricePredictor
    extends KeyedProcessFunction<String, L2UpdateMessage, String> {

  private static final long serialVersionUID = 1L;
  private static final double SMOOTHING = 0.5;

  private transient ValueState<Double> avg;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<Double> avgDescriptor =
        new ValueStateDescriptor<>("average",     // the state name
                                   Types.DOUBLE); // type information

    avg = getRuntimeContext().getState(avgDescriptor);
  }

  @Override
  public void processElement(L2UpdateMessage value, Context ctx,
                             Collector<String> out) throws Exception {

    Double currentAvg = avg.value();

    if (currentAvg == null) {
      currentAvg = 0.0;
    }

    for (Change c : value.getChanges()) {
      currentAvg = SMOOTHING * c.getPrice() + (1 - SMOOTHING) * currentAvg;
    }

    avg.update(currentAvg);

    out.collect(
        String.format("%s Current average price is %.2f\n", value, currentAvg));
  }
}
