import redis

def produce(q_name, msg):
    msg_id = r.xadd(q_name, {'message': msg})

