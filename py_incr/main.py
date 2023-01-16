from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

import os
import json

from river import time_series
from river import utils
from river import metrics


class PriceArima(KeyedProcessFunction):

    def __init__(self):
        self.model = None
        self.metric = None
        self.counter = 0


    def open(self, runtime_context: RuntimeContext):
        self.model = runtime_context.get_state(ValueStateDescriptor(
            "arima model", Types.PICKLED_BYTE_ARRAY()))
        self.metric = runtime_context.get_state(ValueStateDescriptor(
            "metric", Types.PICKLED_BYTE_ARRAY()))
        self.counter = runtime_context.get_state(ValueStateDescriptor(
            "counter", Types.INT()))
        

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        # current = self.state.value()
        # if current is None:
        #     current = Row(value.f1, 0, 0)
        current_model = self.model.value()
        current_metric = self.metric.value()
        c = self.counter.value()
        if c is None:
            c = 0
        c += 1
        self.counter.update(c)
        if current_model is None:
            current_model = time_series.SNARIMAX(
                p=1,
                d=0,
                q=1,
            )
        if current_metric is None:
            current_metric = metrics.MAE()

        buy_pred = current_model.forecast(1)[0]
        rmse = current_metric.update(value['buy_mean'], buy_pred).get()

        new_model = current_model.learn_one(value['buy_mean'])
        self.model.update(new_model)
        self.metric.update(current_metric)
        
        if c < 3000:
            yield {"product_id": value['product_id'],
                "price_type": "SNARIMAX prediction",
                "buy_mean": buy_pred,
                "actual_buy_mean": value['buy_mean'],
                "sell_mean": 0,
                "counter": c-1,
                "rmse": rmse,
                "timestamp": value['timestamp']}

        # write the state back
        # self.state.update(current)

        # schedule the next timer 60 seconds from the current event time


class PriceAverager(KeyedProcessFunction):
    def __init__(self):
        self.sum = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "average",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        # current = self.state.value()
        # if current is None:
        #     current = Row(value.f1, 0, 0)

        changes = value['changes']
        buy_prcies = list(map(lambda x: float(x[1]), filter(
            lambda x: x[0] == 'buy', changes)))
        sell_prices = list(map(lambda x: float(x[1]), filter(
            lambda x: x[0] == 'sell', changes)))

        if len(buy_prcies) == 0:
            buy_mean = 0
        else:
            buy_mean = sum(buy_prcies) / len(buy_prcies)

        if len(sell_prices) == 0:
            sell_mean = 0
        else:
            sell_mean = sum(sell_prices) / len(sell_prices)

        yield {"product_id": value['product_id'],
               "buy_mean": buy_mean,
               "sell_mean": sell_mean,
               "timestamp": value['time'],
               "price_type": "actual price"}


def data_stream_job():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    print(os.getcwd())
    env.add_jars(
        "file:///home/vincent/proj/flinkcoin/consumer/target/flinkcoin-0.1.jar")
    # env.set_python_requirements(requirements_file_path="requirements.txt")
    # env.add_classpaths("file:///opt/flink/jars/flink-connetor-kafka-1.16.0.jar")

    # 2. create source DataStream
    # seq_num_source = NumberSequenceSource(1, 10000)
    # ds = env.from_source(
    #     source=seq_num_source,
    #     watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    #     source_name='seq_num_source',
    #     type_info=Types.LONG())

    kafka_broker = 'kafka1:9092'

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_broker) \
        .set_topics("coinbase") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    parsed_ds = ds.map(lambda a: json.loads(a)) \
        .filter(lambda a: a['type'] == 'l2update') \
        .key_by(lambda a: a['product_id']) \
        .process(PriceAverager()) \
        .filter(lambda a: a['buy_mean'] > 0 and a['sell_mean'] > 0) \

    # parsed_ds.print()

    incr_arima = parsed_ds.key_by(lambda a: a['product_id']) \
        .process(PriceArima())
    incr_arima.print()

    output_path = './log'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()
    incr_arima.sink_to(file_sink)

    # 3. define the execution logic
    # ds = ds.map(lambda a: Row(a % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
    #        .key_by(lambda a: a[0]) \
    #        .map(MyMapFunction(), output_type=Types.TUPLE([Types.LONG(), Types.LONG()]))

    # 4. create sink and emit result to sink

    # ds.sink_to(file_sink)

    # 5. execute the job
    env.execute('data_stream_job')


if __name__ == '__main__':
    data_stream_job()
