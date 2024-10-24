import atexit
from datetime import datetime
import logging
from math import modf
import threading
import time
from typing import Generator
from uuid import uuid4

import grpc
import sympy as sy

from protos import phone_pb2
from protos import phone_pb2_grpc
from src.config import HOST
from src.config import PORT
from src.log import setup_logging

NDIGITS_FOR_ROUND = 6


def input_equation(client, keep_alive=False) -> None:
    while True:
        equation = input("Enter position equation: ").strip()
        try:
            client.equation = equation
            if not keep_alive:
                break
        except Exception as e:
            logging.error(f'Invalid format of equation: "{e}", try again.')


class TelemetryClient:
    def __init__(self, user_id: str, position_equation=None):
        logging.info(f"Initializing TelemetryClient with user id: {user_id}")
        self.user_id = user_id
        self._equation = position_equation
        self.latitude = 0
        self.longitude = 0

    @property
    def equation(self):
        return self._equation

    @equation.setter
    def equation(self, value) -> None:
        self._equation = sy.sympify(value)

    def _calculate_position(self, current_time: float) -> tuple:
        delta = self.equation.evalf(subs={"x": current_time / 3_000_000_000})
        self.latitude += float(delta)
        self.longitude += float(delta)
        return self.latitude, self.longitude

    def _connect(self) -> Generator:
        logging.info("Connecting to server")
        yield phone_pb2.Telemetry(user_id=self.user_id, location=None)

    def _process_stub(
            self,
            stub: phone_pb2_grpc.TelemetryServiceStub,
            response_iterator: Generator,
    ) -> Generator:
        for response in response_iterator:
            if response.HasField("start"):
                response_iterator = stub.SetTelemetryStream(
                    self.generate_telemetry_stream(
                        duration=int(response.start.duration)
                    )
                )
            elif response.HasField("get_one"):
                response_iterator = stub.SetTelemetryStream(
                    self.generate_telemetry_stream(duration=1)
                )
            elif response.HasField("ack"):
                logging.info("Got acknowledge")
        return response_iterator

    def run(self, options: list) -> None:
        t = threading.Thread(target=input_equation, args=(self, True))
        t.start()
        with grpc.insecure_channel(
                f"{HOST}:{PORT}", options=options
        ) as channel:
            stub = phone_pb2_grpc.TelemetryServiceStub(channel)
            response_iterator = stub.SetTelemetryStream(self._connect())
            try:
                while True:
                    response_iterator = self._process_stub(
                        stub, response_iterator
                    )
                    time.sleep(1)
            except Exception as e:
                logging.error(f"Occured an error: {e}")

    def generate_telemetry_stream(self, duration: int) -> None:
        for i in range(duration):
            now = datetime.now().timestamp()
            nanos, seconds = modf(now)
            latitude, longitude = self._calculate_position(now)
            nanos = int(
                round(nanos, NDIGITS_FOR_ROUND) * (10 ** NDIGITS_FOR_ROUND)
            )
            logging.info(
                f"Sending my position: latitude: {latitude}, longitude: {longitude}"
            )
            telemetry = phone_pb2.Telemetry(
                user_id=self.user_id,
                location=phone_pb2.Telemetry.Location(
                    timestamp=phone_pb2.Timestamp(
                        seconds=int(seconds), nanos=nanos
                    ),
                    latitude=latitude,
                    longitude=longitude,
                ),
            )
            yield telemetry
            time.sleep(1)


if __name__ == "__main__":
    setup_logging("app")
    channel_options = [
        ("grpc.keepalive_time_ms", 600 * 1000),
        ("grpc.keepalive_timeout_ms", 600 * 1000),
        ("grpc.client_idle_timeout_ms", 600 * 1000),
    ]
    client = TelemetryClient(user_id=uuid4().hex)
    input_equation(client)
    client.run(options=channel_options)
