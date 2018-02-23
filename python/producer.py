#!/usr/bin/env python
import pika, uuid, sys

class Producer(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.reply_to = 'queue-c'
        self.exchange='exchange-a'
        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.reply_to)

    def on_response(self, ch, method, props, body):
        r = body.decode('utf-8')
        if self.correlation_id == props.correlation_id:
            if r.startswith('a_') > 0:
                self.response_a = int(r.replace('a_',''))
                print('Producer IN (a): {0}'.format(self.response_a))
            else:
                self.response_b = int(r.replace('b_',''))
                print('Producer IN (b): {0}'.format(self.response_b))

    def call(self, n):
        print('Producer OUT: {0}'.format(n))
        self.response_a = None
        self.response_b = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key='',
                                   properties=pika.BasicProperties(
                                         reply_to = self.reply_to,
                                         correlation_id = self.correlation_id),
                                   body=str(n))
        while self.response_a is None or self.response_b is None:
            self.connection.process_data_events()
        return 'Producer OUT: {0}, IN (a): {1}, IN (b): {2}'.format(n, self.response_a, self.response_b)

p = Producer()
n = sys.argv[1] if len(sys.argv) > 1 else 3
r = p.call(n)
print(r)