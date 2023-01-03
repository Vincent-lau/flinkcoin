package uk.ac.cam.sl955.flinkcoin;

import com.github.signaflo.timeseries.*;
import com.github.signaflo.timeseries.model.arima.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PriceArima
    extends ProcessWindowFunction<L2UpdatePrice, String, String, TimeWindow> {

  private static final int P = 1;
  private static final int D = 0;
  private static final int Q = 1;

  @Override
  public void process(String key, Context context,
                      Iterable<L2UpdatePrice> prices, Collector<String> out) {

    double[] series = StreamSupport.stream(prices.spliterator(), false)
                          .mapToDouble(L2UpdatePrice::getBuyPrice)
                          .toArray();

    TimeSeries ts =
        TimeSeries.from(new TimePeriod(TimeUnit.SECOND, 3),
                        prices.iterator().next().getTimeStr(), series);

    Arima model = Arima.model(ts, ArimaOrder.order(P, D, Q));
    out.collect(model.fittedSeries().toString());
  }
}
