package uk.ac.cam.sl955.flinkcoin;

import com.github.signaflo.timeseries.*;
import com.github.signaflo.timeseries.model.arima.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriceArima
    extends ProcessWindowFunction<L2UpdatePrice, L2PredictedPrice, String,
                                  TimeWindow> {

  private static final int P = 1;
  private static final int D = 0;
  private static final int Q = 1;

  private transient ValueState<Arima> prevBuyModel;
  private transient ValueState<Arima> prevSellModel;

  private static final Logger logger =
      LoggerFactory.getLogger(PriceArima.class);

  private Arima getModel(double[] series, TimeSeries ts) {

    Arima model = Arima.model(ts, ArimaOrder.order(P, D, Q));
    logger.debug("original:" + ts.asList().toString() +
                 "size: " + ts.asList().size());

    return model;
  }

  private double getPredictedPrice(Arima model) {
    return model.forecast(1).pointEstimates().at(0);
  }

  private double getMAE(List<Double> l) {
    return l.stream().mapToDouble(x -> Math.abs(x)).sum() / l.size();
  }

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<Arima> buyModelDescriptor = new ValueStateDescriptor<>(
        "buy model",                 // the state name
        Types.GENERIC(Arima.class)); // type information

    prevBuyModel = getRuntimeContext().getState(buyModelDescriptor);

    ValueStateDescriptor<Arima> sellModelDescriptor =
        new ValueStateDescriptor<>(
            "sell model",                // the state name
            Types.GENERIC(Arima.class)); // type information

    prevSellModel = getRuntimeContext().getState(sellModelDescriptor);
  }

  @Override
  public void process(String key, Context context,
                      Iterable<L2UpdatePrice> prices,
                      Collector<L2PredictedPrice> out) throws Exception {

    double[] buySeries = StreamSupport.stream(prices.spliterator(), false)
                             .mapToDouble(L2UpdatePrice::getBuyPrice)
                             .toArray();
    TimeSeries buyTs =
        TimeSeries.from(new TimePeriod(TimeUnit.SECOND, 3),
                        prices.iterator().next().getTimeStr(), buySeries);
    Arima buyModel = getModel(buySeries, buyTs);
    double predictedBuyPrice = getPredictedPrice(buyModel);

    double[] sellSeries = StreamSupport.stream(prices.spliterator(), false)
                              .mapToDouble(L2UpdatePrice::getSellPrice)
                              .toArray();
    TimeSeries sellTs =
        TimeSeries.from(new TimePeriod(TimeUnit.SECOND, 3),
                        prices.iterator().next().getTimeStr(), sellSeries);
    Arima sellModel = getModel(sellSeries, sellTs);
    double predictedSellPrice = getPredictedPrice(sellModel);

    Arima pbm = prevBuyModel.value();
    Arima psm = prevSellModel.value();
    double buyMAE = 0;
    double sellMAE = 0;
    if (pbm != null && psm != null) {
      TimeSeries buyPredTs = pbm.forecast(buySeries.length).pointEstimates();
      TimeSeries sellPredTs = psm.forecast(sellSeries.length).pointEstimates();

      buyMAE = getMAE(buyPredTs.minus(buyTs).asList());
      sellMAE = getMAE(sellPredTs.minus(sellTs).asList());

      if (buyMAE > 1e3|| sellMAE > 1e3) {
        logger.info("buy pred:" + buyPredTs.asList().toString() + "\n"
                    + " buy actual:" + buyTs.asList().toString());
        logger.info("sell pred: {}, \n sell actual: {}", sellPredTs.asList(),
                    sellTs.asList());
        logger.warn("buy error {}. sell error {}.", buyMAE, sellMAE);
        logger.warn(
            "buy pred error" + buyModel.predictionErrors().sumOfSquares() +
            " sell pred error" + sellModel.predictionErrors().sumOfSquares());
      }
    }

    prevBuyModel.update(buyModel);
    prevSellModel.update(sellModel);

    long endTs = context.window().getEnd();

    out.collect(new L2PredictedPrice(prices.iterator().next().getProductId(),
                                     endTs, predictedBuyPrice,
                                     predictedSellPrice, buyMAE, sellMAE));
  }
}
