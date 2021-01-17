#! venv/bin/python

from typing import AsyncIterable, Iterable
from enum import Enum
import logging
import sys
import yaml
import asyncio
from asyncio_mqtt import Client, MqttError
from paho.mqtt.client import MQTTMessage
from contextlib import AsyncExitStack
import datetime
import re
import os


class CurrentStatus(Enum):
    IDLE = 0
    SCHEDULED_WATERING = 1
    MANUAL_WATERING = 2
    TANK_EMPTY = 3


class WateringManager:

    def __init__(self, watering_config_file: str, mqtt_config_file: str):
        # Load config.
        with open(watering_config_file, 'r') as radio_config_file_content:
            self.__watering_config = yaml.safe_load(radio_config_file_content)
        with open(mqtt_config_file, 'r') as mqtt_config_file_content:
            self.__mqtt_config = yaml.safe_load(mqtt_config_file_content)
        # Setup MQTT client.
        self.__client = None
        # Setup current status.
        self.__current_status = CurrentStatus.IDLE
        # Initialize next watering timestamp.
        self.__next_watering_timestamp = None
        self.__init_next_watering_timestamp()
        # Enter loop.
        asyncio.run(self.__loop())

    async def __loop(self):
        await asyncio.gather(self.__mqtt_loop(), self.__next_watering_loop(), self.__update_clock_loop())

    async def __mqtt_loop(self):
        while True:
            try:
                await self.__connect_to_mqtt_broker_and_subscribe_to_topics()
            except MqttError as error:
                print(f'MQTT Error "{error}". Reconnecting in {self.__mqtt_config["reconnect_interval"]} seconds.')
            finally:
                await asyncio.sleep(self.__mqtt_config["reconnect_interval"])

    async def __next_watering_loop(self):
        await asyncio.sleep(1)
        while True:
            now_timestamp = datetime.datetime.now().astimezone().timestamp()
            if now_timestamp - self.__next_watering_timestamp > 5 * 60:
                self.__compute_new_next_watering_timestamp()
            await self.__send_next_watering_timestamp_to_watering_controller()
            await asyncio.sleep(10 * 60)  # Run every 10 minutes.

    async def __update_clock_loop(self):
        await asyncio.sleep(1)
        while True:
            self.__update_clock_of_watering_controller()
            await asyncio.sleep(24*60*60)  # Run once a day.

    def __init_next_watering_timestamp(self):
        # If next watering timestamp is unknown, try to recover from file.
        if self.__next_watering_timestamp is None:
            if os.path.isfile("./next_watering"):
                with open("./next_watering", "r") as file:
                    next_watering_from_file = file.read()
                    if re.fullmatch(r"\d+", next_watering_from_file):
                        next_watering_from_file = int(next_watering_from_file)
                        logging.info(f"Recovered next watering timestamp from file={next_watering_from_file}.")
                        self.__next_watering_timestamp = next_watering_from_file
                    else:
                        logging.warning("Next watering timestamp file is corrupt.")
        # If next watering timestamp is still unknown, compute initial next watering timestamp.
        if self.__next_watering_timestamp is None:
            logging.info(f"Compute initial next watering timestamp.")
            now = datetime.datetime.now().astimezone()
            preferred_watering_hour, preferred_watering_minute = self.__watering_config["preferred_watering_time"]
            next_watering_day = now.day
            if datetime.time(hour=preferred_watering_hour, minute=preferred_watering_minute) < now.time():
                next_watering_day += 1
            next_watering_datetime = datetime.datetime(year=now.year, month=now.month, day=next_watering_day, hour=preferred_watering_hour, minute=preferred_watering_minute, tzinfo=now.tzinfo)
            self.__update_next_watering_timestamp(next_watering_timestamp=int(next_watering_datetime.timestamp()))

    def __compute_new_next_watering_timestamp(self):
        watering_interval = self.__watering_config["watering_interval"]
        logging.info(f"Compute new next watering timestamp with {watering_interval=} days.")
        new_next_watering_timestamp = self.__next_watering_timestamp + watering_interval * 24 * 60 * 60
        self.__update_next_watering_timestamp(next_watering_timestamp=new_next_watering_timestamp)

    def __update_next_watering_timestamp(self, next_watering_timestamp: int):
        logging.info(f"Update next watering timestamp={next_watering_timestamp}.")
        with open("./next_watering", "w") as file:
            file.write(str(next_watering_timestamp))
        self.__next_watering_timestamp = next_watering_timestamp

    async def __send_next_watering_timestamp_to_watering_controller(self):
        logging.info(f"Send next_watering_timestamp={self.__next_watering_timestamp} to watering controller.")
        await self.__send_control_message(message=f"[nextWatering] {self.__next_watering_timestamp}")

    def __update_clock_of_watering_controller(self):
        logging.info(f"Update clock of watering controller.")
        raise NotImplementedError

    async def __handle_watering_messages(self, messages: AsyncIterable[str]):
        async for message in messages:
            if message == "start":
                logging.info(f"[Watering] scheduled watering started.")
                self.__current_status = CurrentStatus.SCHEDULED_WATERING
            elif message == "manual":
                logging.info(f"[Watering] manual watering started.")
                self.__current_status = CurrentStatus.MANUAL_WATERING
            elif message == "end":
                logging.info(f"[Watering] scheduled/manual watering ended.")
                self.__current_status = CurrentStatus.IDLE
            elif message == "skip":
                logging.info(f"[Watering] scheduled/manual watering skipped due to empty tank.")
                self.__current_status = CurrentStatus.TANK_EMPTY
            else:
                logging.warning(f"[Watering] unknown {message=}.")

    async def __handle_tank_messages(self, messages: AsyncIterable[str]):
        async for message in messages:
            if message == "empty":
                logging.info(f"[Tank] tank is empty.")
                self.__current_status = CurrentStatus.TANK_EMPTY
            elif message == "refilled":
                logging.info(f"[Tank] tank was refilled.")
                self.__current_status = CurrentStatus.IDLE
            else:
                logging.warning(f"[Tank] unknown {message=}.")

    async def __handle_moisture_messages(self, messages: AsyncIterable[str]):
        async for message in messages:
            match = re.fullmatch(r"(\d+) (\d+)", message)
            if match:
                first_sensor_value = int(match.group(1))
                second_sensor_value = int(match.group(2))
                logging.warning(f"[Moisture] new measurements: {first_sensor_value=} and {second_sensor_value=}.")
            else:
                logging.warning(f"[Moisture] could not parse {message=}.")

    async def __connect_to_mqtt_broker_and_subscribe_to_topics(self):
        async with AsyncExitStack() as stack:
            mqtt_tasks = set()
            stack.push_async_callback(self.__cancel_async_tasks, mqtt_tasks)
            logging.info(f"Try to connected to MQTT broker \"{self.__mqtt_config['host']}\" at port \"{self.__mqtt_config['port']}\".")
            self.__client = Client(hostname=self.__mqtt_config["host"], port=self.__mqtt_config["port"], username=self.__mqtt_config["user"], password=self.__mqtt_config["password"])
            await stack.enter_async_context(self.__client)
            logging.info(f"Successfully connected to MQTT broker.")
            topic_names = ["watering", "tank", "moisture"]
            topic_messages_handlers = [self.__handle_watering_messages, self.__handle_tank_messages, self.__handle_moisture_messages]
            for topic_name, topic_messages_handler in zip(topic_names, topic_messages_handlers):
                topic = f"{self.__watering_config['topic_root']}/{topic_name}"
                manager = self.__client.filtered_messages(topic)
                messages = await stack.enter_async_context(manager)
                task = asyncio.create_task(topic_messages_handler(self.__decode_mqtt_payload(messages)))
                mqtt_tasks.add(task)
                await self.__client.subscribe(topic)
                logging.info(f"Subscribed to MQTT {topic=}.")
            logging.info(f"Ask watering controller for current status.")
            mqtt_tasks.add(asyncio.create_task(self.__send_control_message("[query] status")))
            await asyncio.gather(*mqtt_tasks)

    async def __send_control_message(self, message: str):
        topic = f"{self.__watering_config['topic_root']}/control"
        logging.info(f"Send MQTT message in {topic=} with payload='{message}'.")
        if self.__client is not None:
            await self.__client.publish(topic=topic, payload=message, qos=2)
        else:
            logging.warning(f"Could not send MQTT message, because the MQTT client is not initialized yet.")

    @staticmethod
    async def __cancel_async_tasks(tasks: Iterable[asyncio.Task]):
        for task in tasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def __decode_mqtt_payload(messages: AsyncIterable[MQTTMessage]):
        async for message in messages:
            yield message.payload.decode()


if __name__ == "__main__":
    # Setup logging.
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
    logging.info("Start raised bed watering manager.")
    watering_config_file = "./watering_config.yaml"
    mqtt_config_file = "./mqtt_config.yaml"
    WateringManager(watering_config_file=watering_config_file, mqtt_config_file=mqtt_config_file)
