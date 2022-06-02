"""
This program is responsible for creating random values of power"""


import sys
sys.path.append("/Users/chandru/rabbit/PV/pv_meter")
from pv_meter.Broker_Producer import Producer_connection
import time
import logging
from pv_meter.Broker_Producer import  Broker_connection


class Meter_rate(Producer_connection):

    def __init__(self, min_power_value: int,
                 max_power_value: int, power_value_generator, broker: Broker_connection):
        # Broker info initialization
        self.__broker = broker

        # Meter info initialization
        self.__power_value_generator = power_value_generator
        self.__min_power_value = min_power_value
        self.__max_power_value = max_power_value
        logging.info("Meter is initialized with Min PV %s MAX PV %s", min_power_value, max_power_value)

    async def connect_with_broker(self):
        await self.__broker.connect_to_server()

    async def publish_message(self, power_value: int) -> None:
        # Sending the power value
        await self.__broker.publish(str(power_value))
        logging.debug("Meter sent %s", power_value)

    def generate_power_value(self) -> int:
        """Generates a random power values between min and max power value."""
        power_value = self.__power_value_generator(self.__min_power_value, self.__max_power_value + 1)
        logging.debug("Generated power value %s", power_value)
        if power_value in range(self.__min_power_value, self.__max_power_value + 1):
            return power_value
        else:
            logging.error("Generated power value is out of range %s-%s", self.__min_power_value, self.__max_power_value)
            return -1

    async def start_publishing(self, producing_interval_seconds):
        logging.info("Meter started to publish power values")
        while True:
            power_value = self.generate_power_value()
            if power_value > 0:
                await self.publish_message(power_value)
                time.sleep(producing_interval_seconds)

    async def close_connection(self):
        await self.__broker.close_connection()
        logging.info("Connection to broker closed by Producer (Meter)")
