import concurrent
from concurrent import futures
import logging
import threading
import time

import grpc

from protos import phone_pb2
from protos import phone_pb2_grpc
from src.log import setup_logging


def command_input_thread(server):
    got_data = False
    while not got_data:
        command = (
            input("Enter command (start <duration>, get_one): ")
            .strip()
            .lower()
        )
        if command == "get_one" or command.startswith("start"):
            server.set_command(command)
            got_data = True
        else:
            print(
                "Invalid command! Please enter 'start <duration>', 'get_one'."
            )


class TelemetryService(phone_pb2_grpc.TelemetryServiceServicer):
    def __init__(self):
        self.command = None

    def set_command(self, command):
        self.command = command

    def _get_command(self):
        return self.command

    def _process_commands(self):
        command = self._get_command()

        if command is not None:
            if command.startswith("start"):
                duration = float(command.split()[1])
                self.set_command(None)
                return phone_pb2.TelemetryStreamCommand(
                    start=phone_pb2.TelemetryStreamCommand.Start(
                        duration=duration
                    )
                )

            elif command == "get_one":
                self.set_command(None)
                return phone_pb2.TelemetryStreamCommand(
                    get_one=phone_pb2.TelemetryStreamCommand.GetOne()
                )

    def SetTelemetryStream(self, request_iterator, context):
        for telemetry in request_iterator:
            logging.info(
                f"Received telemetry from user {telemetry.user_id}: "
                f"Location({telemetry.location.latitude}, {telemetry.location.longitude}) "
                f"at {telemetry.location.timestamp.seconds} seconds and {telemetry.location.timestamp.nanos} nanoseconds"
            )

            yield phone_pb2.TelemetryStreamCommand(
                ack=phone_pb2.TelemetryStreamCommand.Ack()
            )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            command_input_thread(self)
            future = executor.submit(self._process_commands)
            return_value = future.result()
            yield return_value


def serve():
    server_options = [
        ("grpc.keepalive_time_ms", 600 * 1000),
        ("grpc.keepalive_timeout_ms", 600 * 1000),
        ("grpc.max_connection_idle_ms", 600 * 1000),
    ]
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10), options=server_options
    )
    telemetry_service = TelemetryService()
    phone_pb2_grpc.add_TelemetryServiceServicer_to_server(
        telemetry_service, server
    )
    logging.info("Server starting on port 50051.")
    server.add_insecure_port("[::]:50051")
    server.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    setup_logging("server")
    serve()
