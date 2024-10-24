from typing import ClassVar as _ClassVar
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class TelemetryStreamCommand(_message.Message):
    __slots__ = ("start", "get_one", "ack")

    class GetOne(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...

    class Start(_message.Message):
        __slots__ = ("duration",)
        DURATION_FIELD_NUMBER: _ClassVar[int]
        duration: float
        def __init__(self, duration: _Optional[float] = ...) -> None: ...

    class Ack(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...

    START_FIELD_NUMBER: _ClassVar[int]
    GET_ONE_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    start: TelemetryStreamCommand.Start
    get_one: TelemetryStreamCommand.GetOne
    ack: TelemetryStreamCommand.Ack
    def __init__(
        self,
        start: _Optional[_Union[TelemetryStreamCommand.Start, _Mapping]] = ...,
        get_one: _Optional[
            _Union[TelemetryStreamCommand.GetOne, _Mapping]
        ] = ...,
        ack: _Optional[_Union[TelemetryStreamCommand.Ack, _Mapping]] = ...,
    ) -> None: ...

class Telemetry(_message.Message):
    __slots__ = ("user_id", "location")

    class Location(_message.Message):
        __slots__ = ("timestamp", "latitude", "longitude")
        TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        LATITUDE_FIELD_NUMBER: _ClassVar[int]
        LONGITUDE_FIELD_NUMBER: _ClassVar[int]
        timestamp: Timestamp
        latitude: float
        longitude: float
        def __init__(
            self,
            timestamp: _Optional[_Union[Timestamp, _Mapping]] = ...,
            latitude: _Optional[float] = ...,
            longitude: _Optional[float] = ...,
        ) -> None: ...

    USER_ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    location: Telemetry.Location
    def __init__(
        self,
        user_id: _Optional[str] = ...,
        location: _Optional[_Union[Telemetry.Location, _Mapping]] = ...,
    ) -> None: ...

class Timestamp(_message.Message):
    __slots__ = ("seconds", "nanos")
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    NANOS_FIELD_NUMBER: _ClassVar[int]
    seconds: int
    nanos: int
    def __init__(
        self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ...
    ) -> None: ...
