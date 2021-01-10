#! venv/bin/python

import logging
import sys
import yaml
import paho.mqtt.client as mqtt
import datetime
import re
from enum import Enum
from threading import Lock
import os
import asyncio


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
        # Setup mqtt.
        self.__client = mqtt.Client()
        self.__client.on_connect = self.__on_connect
        self.__client.on_message = self.__on_message
        self.__client.username_pw_set(self.__mqtt_config["user"], password=self.__mqtt_config["password"])
        logging.info(f"Try to connected to MQTT broker \"{self.__mqtt_config['host']}\" at port \"{self.__mqtt_config['port']}\".")
        self.__client.connect(self.__mqtt_config["host"], self.__mqtt_config["port"], 60)
        # Setup current status.
        self.__current_status_lock = Lock()
        self.__current_status = CurrentStatus.IDLE
        self.__client.loop_start()
        self.__ask_for_current_status()
        # Initialize next watering timestamp.
        self.__next_watering_timestamp = None
        self.__init_next_watering_timestamp()
        # Enter loop.
        asyncio.run(self.__loop())

    async def __loop(self):
        try:
            await asyncio.gather(self.__next_watering_loop(), self.__update_clock_loop())
        finally:
            self.__client.loop_stop()
            sys.exit()

    async def __next_watering_loop(self):
        await asyncio.sleep(1)
        while True:
            now_timestamp = datetime.datetime.now().astimezone().timestamp()
            if now_timestamp - self.__next_watering_timestamp > 5 * 60:
                self.__compute_new_next_watering_timestamp()
            self.__send_next_watering_timestamp_to_watering_controller()
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

    def __send_next_watering_timestamp_to_watering_controller(self):
        logging.info(f"Send next_watering_timestamp={self.__next_watering_timestamp} to watering controller.")
        self.__send_control_message(message=f"[nextWatering] {self.__next_watering_timestamp}")

    def __update_clock_of_watering_controller(self):
        logging.info(f"Update clock of watering controller.")
        raise NotImplementedError

    def __handle_watering_message(self, message: str):
        if message == "start":
            logging.info(f"[Watering] scheduled watering started.")
            self.__set_current_status(CurrentStatus.SCHEDULED_WATERING)
        elif message == "manual":
            logging.info(f"[Watering] manual watering started.")
            self.__set_current_status(CurrentStatus.MANUAL_WATERING)
        elif message == "end":
            logging.info(f"[Watering] scheduled/manual watering ended.")
            self.__set_current_status(CurrentStatus.IDLE)
        elif message == "skip":
            logging.info(f"[Watering] scheduled/manual watering skipped due to empty tank.")
            self.__set_current_status(CurrentStatus.TANK_EMPTY)
        else:
            logging.warning(f"[Watering] unknown {message=}.")

    def __handle_tank_message(self, message: str):
        if message == "empty":
            logging.info(f"[Tank] tank is empty.")
            self.__set_current_status(CurrentStatus.TANK_EMPTY)
        elif message == "refilled":
            logging.info(f"[Tank] tank was refilled.")
            self.__set_current_status(CurrentStatus.IDLE)
        else:
            logging.warning(f"[Tank] unknown {message=}.")

    def __handle_moisture_message(self, message: str):
        match = re.fullmatch(r"(\d+) (\d+)", message)
        if match:
            first_sensor_value = int(match.group(1))
            second_sensor_value = int(match.group(2))
            logging.warning(f"[Moisture] new measurements: {first_sensor_value=} and {second_sensor_value=}.")
        else:
            logging.warning(f"[Moisture] could not parse {message=}.")

    def __ask_for_current_status(self):
        logging.info(f"Ask watering controller for current status.")
        self.__send_control_message("[query] status")

    def __set_current_status(self, status: CurrentStatus):
        with self.__current_status_lock.acquire():
            self.__current_status = status

    def __on_connect(self, client, _userdata, _flags, return_code):
        logging.info(f"Connected to MQTT broker with {return_code=}.")
        for topic_name in ["watering", "tank", "moisture"]:
            topic = f"{self.__watering_config['topic_root']}/{topic_name}"
            client.subscribe(topic)
            logging.info(f"Subscribed to MQTT {topic=}.")

    def __on_message(self, _client, _userdata, msg):
        topic = msg.topic.replace(self.__watering_config["topic_root"] + "/", "")
        payload = msg.payload.decode("utf-8")
        logging.info(f"Received MQTT message in {topic=} with {payload=}.")
        if topic == "watering":
            self.__handle_watering_message(message=payload)
        elif topic == "tank":
            self.__handle_tank_message(message=payload)
        elif topic == "moisture":
            self.__handle_moisture_message(message=payload)

    def __send_control_message(self, message: str):
        topic = f"{self.__watering_config['topic_root']}/control"
        logging.info(f"Send MQTT message in {topic=} with payload='{message}'.")
        self.__client.publish(topic, payload=message, qos=2)


if __name__ == "__main__":
    # Setup logging.
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
    logging.info("Start raised bed watering manager.")
    watering_config_file = "./watering_config.yaml"
    mqtt_config_file = "./mqtt_config.yaml"
    WateringManager(watering_config_file=watering_config_file, mqtt_config_file=mqtt_config_file)
