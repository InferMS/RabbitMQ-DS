from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class airData(_message.Message):
    __slots__ = ["pollution", "wellness"]
    POLLUTION_FIELD_NUMBER: _ClassVar[int]
    WELLNESS_FIELD_NUMBER: _ClassVar[int]
    pollution: _containers.RepeatedCompositeFieldContainer[pollutionData]
    wellness: _containers.RepeatedCompositeFieldContainer[wellnessData]
    def __init__(self, pollution: _Optional[_Iterable[_Union[pollutionData, _Mapping]]] = ..., wellness: _Optional[_Iterable[_Union[wellnessData, _Mapping]]] = ...) -> None: ...

class pollutionData(_message.Message):
    __slots__ = ["coefficient", "id", "timestamp"]
    COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    coefficient: float
    id: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., coefficient: _Optional[float] = ...) -> None: ...

class wellnessData(_message.Message):
    __slots__ = ["coefficient", "id", "timestamp"]
    COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    coefficient: float
    id: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., coefficient: _Optional[float] = ...) -> None: ...
