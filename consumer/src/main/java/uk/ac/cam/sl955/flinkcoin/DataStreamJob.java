package uk.ac.cam.sl955.flinkcoin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

@Slf4j
public class DataStreamJob {
    private static final Gson gson = new GsonBuilder().create();

    public static String getEnvironmentVariable(final String key, final String defaultValue) {
        final String value = System.getenv(key);
        if (value != null) {
            log.info("Value read from environment: {}={}", key, value);
            return value;
        } else {
            log.info("Value not found in environment, using default value: {}={}", key, defaultValue);
            return defaultValue;
        }
    }

    public static void main(String[] args) throws Exception {
        final String kafkaBroker = getEnvironmentVariable("KAFKA_BROKER", "localhost:9094");
        final String topic = getEnvironmentVariable("KAFKA_TOPIC", "coinbase");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBroker);
        properties.setProperty("group.id", "flink-app");

        final DataStream<String> input = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));

        getL2UpdatesStream(input)
                .map(obj -> Tuple2.of(obj.productId, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .sum(1)
                .addSink(new PrintSinkFunction<>());

        env.execute("Flink consumer");
    }

    public static DataStream<L2UpdateMessage> getL2UpdatesStream(final DataStream<String> input) {
        return input.map(JsonParser::parseString)
                .map(JsonElement::getAsJsonObject)
                .filter(obj -> obj.getAsJsonPrimitive("type").getAsString().equals("l2update"))
                .map(obj -> gson.fromJson(obj.toString(), L2UpdateMessage.class));
    }
}