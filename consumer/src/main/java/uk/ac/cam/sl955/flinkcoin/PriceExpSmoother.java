package uk.ac.cam.sl955.flinkcoin;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;

public class PriceExpSmoother
    extends KeyedProcessFunction<String, L2UpdatePrice, L2PredictedPrice> {

  private static final long serialVersionUID = 1L;
  private static final double SMOOTHING = 0.5;

  private transient ValueState<Double> buyAvg;
  private transient ValueState<Double> sellAvg;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<Double> buyAvgDescriptor =
        new ValueStateDescriptor<>("buy average", // the state name
                                   Types.DOUBLE); // type information

    buyAvg = getRuntimeContext().getState(buyAvgDescriptor);

    ValueStateDescriptor<Double> sellAvgDescriptor =
        new ValueStateDescriptor<>("sell average", // the state name
                                   Types.DOUBLE);  // type information

    sellAvg = getRuntimeContext().getState(sellAvgDescriptor);
  }

  @Override
  public void processElement(L2UpdatePrice price, Context ctx,
                             Collector<L2PredictedPrice> out) throws Exception {

    double cBuyAvg, cSellAvg;
    if (buyAvg.value() == null || sellAvg == null) {
      cBuyAvg = cSellAvg = 0.0;
    } else {
      cBuyAvg = buyAvg.value();
      cSellAvg = sellAvg.value();
    }

    double bp = price.getBuyPrice();
    double sp = price.getSellPrice();

    double be = Math.abs(bp - cBuyAvg);
    double se = Math.abs(sp - cSellAvg);

    cBuyAvg = SMOOTHING * bp + (1 - SMOOTHING) * cBuyAvg;
    cSellAvg = SMOOTHING * sp + (1 - SMOOTHING) * cSellAvg;

    buyAvg.update(cBuyAvg);
    sellAvg.update(cSellAvg);

    out.collect(new L2PredictedPrice(price.getProductId(), price.getTimestamp(),
                                     cBuyAvg, cSellAvg, be, se));
  }
}
