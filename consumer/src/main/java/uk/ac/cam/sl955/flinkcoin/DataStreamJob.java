package uk.ac.cam.sl955.flinkcoin;

import com.github.signaflo.timeseries.model.arima.Arima;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
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

    final DataStream<String> input = env.fromSource(
        source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    DataStream<L2UpdateMessage> parsedData = getL2UpdatesStream(input);
    // parsedData.addSink(new PrintSinkFunction<>()).name("PrintSink");

    DataStream<L2UpdatePrice> processedData =
        parsedData.keyBy(L2UpdateMessage::getProductId)
            .process(new PriceAverager())
            .name("processedData");

    // processedData.addSink(new PrintSinkFunction<>()).name("PrintSink");

    processedData
        .map(new RichMapFunction<L2UpdatePrice, InfluxDBPoint>() {
          @Override
          public InfluxDBPoint map(L2UpdatePrice l2) throws Exception {

            String measurement = l2.getProductId();

            long timestamp = l2.getTimeLong();

            HashMap<String, Object> fields = new HashMap<>();
            fields.put("buy", l2.getBuyPrice());
            fields.put("sell", l2.getSellPrice());

            return new InfluxDBPoint(measurement, timestamp,
                                     new HashMap<String, String>(), fields);
          }
        })
        .name("InfluxDB data point")
        .addSink(new InfluxDBSink(influxDBConfig))
        .name("InfluxDBSink");

    // TODO change to event time with watermarks?
    processedData.keyBy(L2UpdatePrice::getProductId)
        .window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
        .process(new PriceArima())
        .addSink(new PrintSinkFunction<>())
        .name("PrintSink");

    // .map(obj -> Tuple2.of(obj.productId, 1))
    // .returns(Types.TUPLE(Types.STRING, Types.INT))
    // .keyBy(tuple2 -> tuple2.f0)
    // .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
    // .sum(1)

    env.execute("Flink consumer");

    // final StreamExecutionEnvironment env =
    //     StreamExecutionEnvironment.getExecutionEnvironment();

    // DataStream<Person> flintstones =
    //     env.fromElements(new Person("Fred", 35), new Person("Wilma", 35),
    //                      new Person("Pebbles", 2));

    // DataStream<Person> adults =
    //     flintstones.filter(new FilterFunction<Person>() {
    //       @Override
    //       public boolean filter(Person person) throws Exception {
    //         return person.age >= 18;
    //       }
    //     });

    // adults.print();

    // env.execute();
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
