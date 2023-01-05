package uk.ac.cam.sl955.flinkcoin;

import com.github.signaflo.timeseries.*;
import com.github.signaflo.timeseries.model.arima.*;
import java.util.*;
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

public class PriceAutoCorr
    extends ProcessWindowFunction<L2UpdatePrice, L2AutoCorr, String,
                                  TimeWindow> {

  @Override
  public void process(String key, Context context,
                      Iterable<L2UpdatePrice> prices,
                      Collector<L2AutoCorr> out) throws Exception {

    double[] buySeries = StreamSupport.stream(prices.spliterator(), false)
                             .mapToDouble(L2UpdatePrice::getBuyPrice)
                             .toArray();
    TimeSeries buyTs =
        TimeSeries.from(new TimePeriod(TimeUnit.SECOND, 3),
                        prices.iterator().next().getTimeStr(), buySeries);
    Map<Integer, Double> buyAutoCorr = new HashMap<>();
    double[] buyAutoCorrArray = buyTs.autoCorrelationUpToLag(3);
    for (int i = 0; i < buyAutoCorrArray.length; i++) {
      buyAutoCorr.put(i, buyAutoCorrArray[i]);
    }

    L2UpdatePrice first = prices.iterator().next();
    out.collect(new L2AutoCorr(first.getProductId(), first.getTimestamp(), buyAutoCorr));

    // TimeSeries sellTs =
    //     TimeSeries.from(new TimePeriod(TimeUnit.SECOND, 3),
    //                     prices.iterator().next().getTimeStr(), sellSeries);
  }
}
