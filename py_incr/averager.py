import datetime

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment


class PriceAverager(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        # current = self.state.value()
        # if current is None:
        #     current = Row(value.f1, 0, 0)

        changes = value['changes']
        buy_prcies = (map(lambda x: x[1], filter(
            lambda x: x[0] == 'buy', changes)))
        sell_prices = (map(lambda x: x[1], filter(
            lambda x: x[0] == 'sell', changes)))

        buy_mean = sum(buy_prcies) / len(buy_prcies)
        sell_mean = sum(sell_prices) / len(sell_prices)

        yield {"product_id": value['product_id'], "buy_mean": buy_mean, "sell_mean": sell_mean, "timestamp": value['time']}

        # write the state back
        # self.state.update(current)

        # schedule the next timer 60 seconds from the current event time
