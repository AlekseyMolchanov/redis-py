from typing import List, Optional


class Stream:
    def __init__(self, data: dict) -> None:
        self.length = data['length']
        self.RadixTreeKeys = data['radix-tree-keys']
        self.RadixTreeNodes = data['radix-tree-nodes']
        self.LastGeneratedID = data['last-generated-id']
        self.Groups = data['groups']
        self.FirstEntry = StreamMessage(*data['first-entry'])
        self.LastEntry = StreamMessage(*data['last-entry'])


class Group:
    def __init__(self, data: dict) -> None:
        self.Name = data['name']
        self.Consumers = data['consumers']
        self.Pending = data['pending']
        self.LastDeliveredID = data['last-delivered-id']


class Consumer:
    def __init__(self, data: dict) -> None:
        self.Name = data['name']
        self.Pending = data['pending']
        self.Idle = data['idle']


class StreamMessage:
    def __init__(self, message_id: bytes, data: dict) -> None:
        self.message_id = message_id.decode()
        self.data = data

    def __repr__(self) -> str:
        return f"<{self.message_id}>: {self.data}"


class ConsumerStream:
    def __init__(self, data: List) -> None:
        __strem_name, __messages = data
        self.name = __strem_name
        self.messages = __messages

    @property
    def message(self) -> Optional[StreamMessage]:
        if self.messages:
            return StreamMessage(*self.messages[0])


class ConsumerStreams:
    def __init__(self, data: List) -> None:
        self.__streams = list(map(ConsumerStream, data))

    def get_message(self) -> Optional[StreamMessage]:
        if self.__streams:
            __stream: ConsumerStream = self.__streams[0]
            return __stream.message

    def has_message(self) -> bool:
        return bool(self.get_message())


class QStatus:
    def __init__(self, info: Stream, consumers: List[Consumer], groups: List[Group]) -> None:
        self.Info = info
        self.Consumers: List[Consumer] = consumers
        self.Groups: List[Group] = groups
