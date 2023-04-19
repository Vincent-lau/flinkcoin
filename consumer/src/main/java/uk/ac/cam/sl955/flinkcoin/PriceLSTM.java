package uk.ac.cam.sl955.flinkcoin;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;

public class PriceLSTM
    extends KeyedProcessFunction<String, L2UpdateMessage, L2UpdatePrice> {

  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(L2UpdateMessage value, Context ctx,
                             Collector<L2UpdatePrice> out) throws Exception {

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

    out.collect(
        new L2UpdatePrice(value.getProductId(), value.getTimestamp(), bp, sp));
  }
}
