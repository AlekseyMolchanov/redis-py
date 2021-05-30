
from functools import wraps

from redis.exceptions import RedisError


def protect_exception(method):
    @wraps(method)
    def __wrapper(self, *args, **kwarg):
        try:
            __result = method(self, *args, **kwarg)
            return __result
        except RedisError as ex:
            __error = f"failed to call method: {method} '{args}' '{kwarg}', error : {ex}"
            self.log.error(__error)
            raise NewRdbException from ex
    return __wrapper


class NewRdbException(Exception):
    """ NewRdb base Exception """
    pass
