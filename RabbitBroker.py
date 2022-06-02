"""
The class here displays the Async Communication with RabbitMQ as broker.
"""


import sys
sys.path.append("/Users/chandru/rabbit/PV/pv_meter")
from pv_meter.Broker import Broker_connection
from aio_pika import connect, Message
import logging


class AsyncBroker(Broker_connection):

    __connection = object
    __channel = object
    __broker_queue = object

    def __init__(self, broker_address: str, broker_queue_name: str):
        """
        :parameter broker_address:
        :parameter broker_queue_name:
        """
        # initialize broker properties
        self.__broker_address = broker_address
        self.__broker_queue_name = broker_queue_name
        logging.info("Broker address: %s and Queue %s set",  broker_address, broker_queue_name)

    async def connect_to_server(self):
        # Perform connection with broker
        self.__connection = await connect(self.__broker_address)

        # Creating a channel for broker
        self.__channel = await self.__connection.channel()

        # Declaring queue on rabbitmq
        self.__broker_queue = await self.__channel.declare_queue(self.__broker_queue_name)
        logging.info("Connection to server: %s achieved.", self.__broker_address)

    async def publish(self, message: str):
        logging.debug("Publishing %s", message)
        await self.__channel.default_exchange.publish(
            Message(message.encode('utf-8')),
            routing_key=self.__broker_queue_name
        )

    async def register_consumer(self, consumer):
        await self.__broker_queue.consume(consumer.on_message, no_ack=True)
        logging.info("%s is registered here as consumer", consumer.__str__())

    async def close_connection(self):
        await self.__connection.close()
        logging.info("Connection to the server is closed.")
