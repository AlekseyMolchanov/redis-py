import logging
from typing import List, Optional, Tuple

from redisintf.schema import (Consumer, ConsumerStreams, Group, QStatus,
                              Stream, StreamMessage)

from .constants import DEFAULT_GRP, LOGGER_NAME
from .exceptions import NewRdbException, protect_exception
from .redis import Redis62, Redis62Error


class NewRdb:

    GRP = DEFAULT_GRP

    def __init__(self, **kvargs):
        self.__redis: Redis62 = Redis62(**kvargs)
        self.log = logging.getLogger(LOGGER_NAME)

    def close(self):
        # Close closes the redis connection when called
        self.__redis.close()
        self.__redis.connection_pool.disconnect()
        self.__redis.connection_pool.max_connections = 0

    @protect_exception
    def value_set(self, key: str, value: str, expiry: Optional[int] = None) -> bool:
        # Set will provide a way to set a key in redis

        if expiry:
            result = self.__redis.setex(key, expiry, value)
        else:
            result = self.__redis.set(key, value)

        return result

    @protect_exception
    def value_get(self, key: str) -> str:
        # Get will get the messages with provided key
        return self.__redis.get(key)

    @protect_exception
    def produce(self, stream_name: str, msg: bytes,
                maxlen: Optional[int] = None,
                approximate: bool = True) -> str:
        # Produce will push the message to stream for the consumers
        result: str = self.__redis.xadd(stream_name,
                                        {'message': msg},
                                        id='*',
                                        maxlen=maxlen,
                                        approximate=approximate).decode()

        __debug = f"message pushed to stream {stream_name} - res : {result}"
        self.log.debug(__debug)

        return result  # {millisecondsTime}-{sequenceNumber}

    def group_name(self, stream_name: str) -> str:
        return f"{stream_name}{self.GRP}"

    def __consume(self,
                  stream_name: str,
                  group_name: str,
                  name: str,
                  key: str, block: Optional[int] = None) -> ConsumerStreams:

        try:
            data = self.__redis.xreadgroup(
                group_name,
                name,
                {stream_name: key},
                count=1,
                block=block,  # Millisecond
                noack=False
            )
            __debug = f"message {data} consumed from stream '{stream_name}'  by consumer '{name}'.'{group_name}'"
            self.log.debug(__debug)

            return ConsumerStreams(data)

        except Redis62Error as ex:
            raise NewRdbException from ex

    @protect_exception
    def consume(self,
                stream_name: str,
                name: str,
                block: Optional[int] = None) -> StreamMessage:

        group_name = self.group_name(stream_name)

        __block = (block or 500)

        response = self.__consume(stream_name, group_name, name, "0", __block)

        if not response.has_message():
            response = self.__consume(stream_name, group_name, name, ">", __block)

        return response.get_message()

    @protect_exception
    def consumer_info(self, stream_name: str) -> List[Consumer]:
        group_name = self.group_name(stream_name)
        consumers_data = self.__redis.xinfo_consumers(stream_name, group_name)
        return [Consumer(each) for each in consumers_data]

    @protect_exception
    def ack(self, stream_name: str, ids: List[str]):
        group_name = self.group_name(stream_name)
        result = self.__redis.xack(stream_name, group_name, *ids)
        __debug = f"message acknowledge successfully for message {ids} - res : {result}"
        self.log.debug(__debug)
        return result

    @protect_exception
    def group_exists(self, stream_name: str) -> bool:
        # GrpExists verifies if provided group Exists or not
        # it also verifies that the stream we are connecting
        # exists if not it will return an error

        group_name = self.group_name(stream_name)

        __groupname = group_name.encode()
        __groups = self.__redis.xinfo_groups(stream_name)
        return bool(len(list(
            filter(lambda group: group['name'] == __groupname, __groups))
        ))

    @protect_exception
    def group_info(self, stream_name: str) -> List[Group]:
        groups_data = self.__redis.xinfo_groups(stream_name)
        return [Group(each) for each in groups_data]

    @protect_exception
    def group_create(self, stream_name: str,
                     persistent: Optional[bool] = False,
                     start: Optional[str] = None) -> bool:
        # CreateGrp creates a new group for provided stream

        if persistent:
            raise NewRdbException('fatal error! persistence not supported')

        group_name = self.group_name(stream_name)

        if self.group_exists(stream_name):
            return True

        result = self.__redis.xgroup_create(stream_name,
                                            group_name,
                                            mkstream=True,  # TODO: required?
                                            id=(start or '$'))

        __debug = f"group '{group_name}' for stream '{stream_name}' created: {result}'"
        self.log.debug(__debug)

        return result

    @protect_exception
    def group_delete(self, stream_name: str) -> bool:
        # DeleteGrp creates a new group for provided stream
        group_name = self.group_name(stream_name)
        result = self.__redis.xgroup_destroy(stream_name, group_name)

        __debug = f"group '{group_name}' for stream '{stream_name}' destroyed: {result}"
        self.log.debug(__debug)

        return result

    @protect_exception
    def stream_exists(self, stream_name: str) -> bool:
        # StreamExists checks if a stream with given name Exists or not
        __stream_name = stream_name.encode()
        streams = self.__redis.scan(_type='STREAM')
        return __stream_name in streams

    @protect_exception
    def stream_info(self, stream_name: str) -> Stream:
        stream_data = self.__redis.xinfo_stream(stream_name)
        return Stream(stream_data)

    @protect_exception
    def stream_delete(self, stream_name: str):
        # DeleteStream will delete the whole stream with it's data
        group_name = self.group_name(stream_name)
        result = self.__redis.delete(stream_name)
        self.__redis.delete(f"{stream_name}:{group_name}:acknowledge")
        __debug = f"stream delete '{stream_name}': {result}"
        self.log.debug(__debug)

    def q_delete(self, stream_name: str):
        self.group_delete(stream_name)
        self.stream_delete(stream_name)

    def q_status(self, stream_name: str) -> QStatus:
        return QStatus(
            info=self.stream_info(stream_name),
            consumers=self.consumer_info(stream_name),
            groups=self.group_info(stream_name)
        )

    @protect_exception
    def pending(self, stream_name: str, idleTime: int) -> Tuple[dict, int]:
        group_name = self.group_name(stream_name)
        pended = self.__redis.xpending_extended(
            stream_name,
            group_name,
            "-",
            "+",
            10,
            idle=idleTime
        )
        count = 0
        res = dict()
        for message in pended:
            consumer = message['consumer'].decode()
            message_id = message['message_id'].decode()
            res.setdefault(consumer, []).append(message_id)
            count += 1

        if count:
            self.log.warning(f"pended messages '{count}': {res}")

        return res, count

    def claim_pended_messages(self, stream_name: str, consumer: str, idleTime: int) -> bool:
        pended_messages, _ = self.pending(stream_name, idleTime)
        for pended_consumer, messages in pended_messages.items():
            if pended_consumer != consumer:
                message = messages[0]
                self.claim_messages(stream_name, consumer, [message])
                __debug = f"messages claimed!  inActiveConsumer: {pended_consumer}, newConsumer: {consumer}, msgId: {message}"
                self.log.debug(__debug)
                return True
        return False

    @protect_exception
    def claim_messages(self, stream_name: str, consumer: str, ids: List[str],
                       min_idle_time: Optional[int] = None):

        group_name = self.group_name(stream_name)
        result = self.__redis.xclaim(
            stream_name,
            group_name,
            consumer,
            min_idle_time or 0,
            ids
        )

        self.log.debug(f"message claimed - {result}")
        return result

    # aliases
    Close = close

    Set = value_set
    Get = value_get

    Produce = produce
    Consume = consume
    getConsumerInfo = consumer_info

    Ack = ack

    GrpExists = group_exists
    CreateGrp = group_create
    DeleteGrp = group_delete
    getGroupInfo = group_info

    StreamExists = stream_exists
    DeleteStream = stream_delete
    getStreamInfo = stream_info

    DeleteQ = q_delete
    GetQStats = q_status

    PendingStreamMessages = pending
    claimMessages = claim_messages
    ClaimPendingMessages = claim_pended_messages
