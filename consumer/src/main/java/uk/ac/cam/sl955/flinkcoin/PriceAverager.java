package uk.ac.cam.sl955.flinkcoin;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;

public class PriceAverager
    extends KeyedProcessFunction<String, L2UpdateMessage, L2UpdatePrice> {

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
                             Collector<L2UpdatePrice> out) throws Exception {

    // Double currentAvg = avg.value();

    // if (currentAvg == null) {
    //   currentAvg = 0.0;
    // }

    double bp = value.getChanges()
                    .stream()
                    .filter(c -> c.getSide() == Side.BUY)
                    .mapToDouble(Change::getPrice)
                    .average()
                    .orElse(0.0);

    double sp = value.getChanges()
                    .stream()
                    .filter(c -> c.getSide() == Side.SELL)
                    .mapToDouble(Change::getPrice)
                    .average()
                    .orElse(0.0);

    out.collect(new L2UpdatePrice(value, bp, sp));
  }
}
