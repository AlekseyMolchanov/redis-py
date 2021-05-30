import redis
from redis._compat import long
from redis.exceptions import DataError, RedisError


class Redis62Error(RedisError):
    pass


class Redis62(redis.Redis):
    """
    Support Redis 6.2+
    """

    def xpending_extended(self, name, groupname, min, max, count,
                          consumername=None, idle=None):
        """
        XPENDING mystream group55 IDLE 9000 - + 10 consumer-123

        Returns information about pending messages, in a range.
        name: name of the stream.
        groupname: name of the consumer group.
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).

        idle: Millisecond, Since version 6.2 it is possible to filter entries by their idle-time,
        given in milliseconds (useful for XCLAIMing entries that have not been
        processed for some time)

        """
        pieces = [name, groupname]

        if idle is not None:
            pieces.extend(['IDLE', idle])

        if min is not None or max is not None or count is not None:
            if min is None or max is None or count is None:
                raise DataError("XPENDING must be provided with min, max "
                                "and count parameters, or none of them. ")
            if not isinstance(count, (int, long)) or count < -1:
                raise DataError("XPENDING count must be a integer >= -1")
            pieces.extend((min, max, str(count)))
        if consumername is not None:
            if min is None or max is None or count is None:
                raise DataError("if XPENDING is provided with consumername,"
                                " it must be provided with min, max and"
                                " count parameters")
            pieces.append(consumername)
        return self.execute_command('XPENDING', *pieces, parse_detail=True)
