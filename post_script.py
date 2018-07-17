import requests
import time
import multiprocessing
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import queue


def requester(url, data, headers):
    data['sys_ts'] = time.time()
    r = requests.post(url, json = data, headers=headers)


def sender(url, data, headers, number_req, q_req):
    """
    Posts specified number of requests on "separate" threads
    """

    for idx, number in enumerate(range(number_req),1):
        t = threading.Thread(target=requester, args=(url,data,headers))
        t.start()
        q_req.put(idx)


def receiver(q):
    """
    Sets up the Kafka listener to handle requests
    """

    conf = {'bootstrap.servers': BROKER, 'group.id': GROUP, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    c = Consumer(conf)
    running = True


    try:
        c.subscribe([TOPIC])
        while running:
            msg = c.poll(timeout=1.0)

            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% Reached end of topic {0} [{1}] at offset {2}\n'.format(
                                    msg.topic(), msg.partition(), msg.offset()))
                    continue
                else:
                    raise KafkaException(msg.error())
                    break
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            msgv = msg.value()
            q.put(msgv)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        print("%% Closing consumer\n")
        c.close()


def reader(q):
    reader_list = []
    while not q.empty():
        test_get = q.get()
        reader_list.append(test_get)
    return reader_list


if __name__ == "__main__":
    TOPIC = "bar"
    BROKER = "kafka:9092"
    GROUP = "foo"
    url = "http://localhost:8080/data/{}".format(TOPIC)
    data = {'node_id':'00000000-0000-0000-0000-000000002977'}
    headers = {'Content-type': 'application/json'}
    q = multiprocessing.Queue()
    q_req = multiprocessing.Queue()


    p_receiver = multiprocessing.Process(target=receiver, args=(q,))
    p_sender = multiprocessing.Process(target=sender, args=(url, data, headers, 2, q_req))

    p_receiver.start()
    p_sender.start()
    p_receiver.join()
    p_sender.join()

    items_sent = len(reader(q_req))
    items_received = len(reader(q))
    print("No. items sent to Kafka: {}.".format(items_sent))
    print("No. items received from Kafka: {}.".format(items_received))
