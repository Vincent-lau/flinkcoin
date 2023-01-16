package uk.ac.cam.sl955.flinkcoin;

import com.github.signaflo.timeseries.model.arima.Arima;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
  private static final Gson gson = new GsonBuilder().create();
  private static final Logger log =
      LoggerFactory.getLogger(DataStreamJob.class);

  public static final String HOST_IP = "172.18.0.1";

  public static String getEnvironmentVariable(final String key,
                                              final String defaultValue) {
    final String value = System.getenv(key);
    if (value != null) {
      log.info("Value read from environment: {}={}", key, value);
      return value;
    } else {
      log.info("Value not found in environment, using default value: {}={}",
               key, defaultValue);
      return defaultValue;
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Starting Flink consumer");

    // final String hostIP = getEnvironmentVariable("HOST_IP", "localhost");
    final String kafkaHost = "kafka1";
    final String kafkaBroker = kafkaHost + ":9092";

    final String topic = getEnvironmentVariable("KAFKA_TOPIC", "coinbase");

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    final Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaBroker);
    properties.setProperty("group.id", "flink-app");

    // connect to Kafka
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBroker)
            .setTopics(topic)
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    InfluxDBConfig influxDBConfig =
        InfluxDBConfig
            .builder("http://" + HOST_IP + ":8086", "vincent", "20040209",
                     "coinbase")
            .batchActions(1000)
            .flushDuration(100, TimeUnit.MILLISECONDS)
            .enableGzip(true)
            .build();

    final DataStream<String> input =
        env.fromSource(source,
                       WatermarkStrategy.<String>forBoundedOutOfOrderness(
                           Duration.ofMillis(100)),
                       "Kafka Source");

    DataStream<L2UpdateMessage> parsedData = getL2UpdatesStream(input);

    DataStream<L2UpdatePrice> processedData =
        parsedData.keyBy(L2UpdateMessage::getProductId)
            .process(new PriceAverager())
            .filter(L2UpdatePrice::validPrice)
            .name("processedData");



    processedData.map(new L2ToInflux())
        .name("InfluxDB data point")
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("InfluxDBSink");

    processedData.keyBy(L2UpdatePrice::getProductId)
        .process(new PriceExpSmoother())
        .map(new L2PredToInflux(PriceType.ES_PRED))
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("exp smoothing prediction sink");

    processedData.keyBy(L2UpdatePrice::getProductId)
        .process(new PriceExpSmoother())
        .map(new L2ErrToInflux(PriceType.ES_ERR))
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("exp smoothing err sink");

    processedData.keyBy(L2UpdatePrice::getProductId)
        .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
        .process(new PriceArima())
        .map(new L2PredToInflux(PriceType.ARIMA_PRED))
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("arima prediction sink");

    processedData.keyBy(L2UpdatePrice::getProductId)
        .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
        .process(new PriceArima())
        .map(new L2ErrToInflux(PriceType.ARIMA_ERR))
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("arima prediction err sink");

    // processedData.keyBy(L2UpdatePrice::getProductId)
    //     .window(
    //         SlidingProcessingTimeWindows.of(Time.seconds(3),
    //         Time.seconds(1)))
    //     .process(new PriceAutoCorr())
    //     .map(new AR2Influx())
    //     .addSink(new InfluxDBSink(influxDBConfig))
    //     .name("prediction sink");

    env.execute("Flink consumer");
  }

  public static DataStream<L2UpdateMessage>
  getL2UpdatesStream(final DataStream<String> input) {
    return input.map(JsonParser::parseString)
        .map(JsonElement::getAsJsonObject)
        .filter(
            obj
            -> obj.getAsJsonPrimitive("type").getAsString().equals("l2update"))
        .map(obj -> gson.fromJson(obj.toString(), L2Update.class))
        .map(L2UpdateMessage::new)
        .name("L2UpdateMessage");
  }
}
