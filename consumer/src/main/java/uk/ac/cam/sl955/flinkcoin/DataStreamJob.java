package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Properties;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
  private static final Gson gson = new GsonBuilder().create();
  private static final Logger log =
      LoggerFactory.getLogger(DataStreamJob.class);

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
    final String hostIP = "kafka1";

    final String kafkaBroker = hostIP + ":9092";
    final String topic = getEnvironmentVariable("KAFKA_TOPIC", "coinbase");

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    final Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaBroker);
    properties.setProperty("group.id", "flink-app");

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBroker)
            .setTopics(topic)
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    final DataStream<String> input = env.fromSource(
        source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    // final DataStream<String> input = env.addSource(
    //     new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(),
    //     properties));

    getL2UpdatesStream(input)
        .map(obj -> Tuple2.of(obj.productId, 1))
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .keyBy(tuple2 -> tuple2.f0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
        .sum(1)
        .addSink(new PrintSinkFunction<>())
        .name("PrintSink");

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
        .map(obj -> gson.fromJson(obj.toString(), L2UpdateMessage.class))
        .name("L2UpdateMessage");
  }

  public static class Person {
    public String name;
    public Integer age;
    public Person() {}

    public Person(String name, Integer age) {
      this.name = name;
      this.age = age;
    }

    public String toString() {
      return this.name.toString() + ": age " + this.age.toString();
    }
  }
}