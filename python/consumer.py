#!/usr/bin/env python
import pika, sys, time
from random import randint

credentials = pika.PlainCredentials('dsv', 'dsv')
connection = pika.BlockingConnection(pika.ConnectionParameters('as1397.lojasrenner.com.br', 5672, '/', credentials))
channel = connection.channel()
_exchange='exchange-a'
_queuea = 'queue-a'
channel.exchange_declare(exchange=_exchange, exchange_type='fanout', durable=True)
_queue = sys.argv[1] if len(sys.argv) > 1 else _queuea
channel.queue_declare(queue=_queue, durable=True) 
channel.queue_bind(exchange=_exchange, queue=_queue)

def on_request(ch, method, props, body):
    n = int(body)
    print('Consumer IN: {0}'.format(n))
    x = randint(1, 10)
    r = 'b_{0}'.format(n * x)
    if _queue.startswith(_queuea) > 0:
        r = 'a_{0}'.format(n + x)
        time.sleep(5)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=str(r))
    print('Consumer OUT: {0}'.format(r))
    print()
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1) # In order to spread the load equally over multiple servers
channel.basic_consume(on_request, queue=_queue)

print('Consumer WAIT ({0})'.format(_queue))
channel.start_consuming()