import getopt
import pickle
import random
import sys
import threading
import time
import terminal
import multiprocessing
import grpc
from concurrent import futures
import redis

import server_consumer, sensor_producer
import proxy


def main():
    pollutionSensors = 1
    qualitySensors = 1
    servers_num = 2

    argv = sys.argv[1:]

    opts, args = getopt.getopt(argv, "p:q:s:t:",
                               ["pollution_sensor=",
                                "quality_sensor=",
                                "servers=",
                                "terminals="])

    for opt, arg in opts:
        if opt in ['-p', '--pollution_sensor']:
            pollutionSensors = arg
        elif opt in ['-q', '--quality_sensor']:
            qualitySensors = arg
        elif opt in ['-s', '--servers']:
            servers_num = arg
        elif opt in ['-t', '--terminals']:
            terminals = arg

    r = redis.Redis(host='localhost', port=6379)
    pollution = dict()
    wellness = dict()

    pollution_bytes = pickle.dumps(pollution)
    wellness_bytes = pickle.dumps(wellness)

    r.set('pollution', pollution_bytes)
    r.set('wellness', wellness_bytes)

    threads = []

    servers = []
    for index in range(int(servers_num)):
        thread = threading.Thread(
            target=server_consumer.start,
            args=(index,))
        thread.start()
        threads.append(thread)

    randomList = []
    for index in range(int(qualitySensors)):
        success = False
        while not success:
            sensorId = random.randint(1, 999)
            if sensorId not in randomList:
                randomList.append(sensorId)
                thread = threading.Thread(
                    target=sensor_producer.sendMeteoData,
                    args=(sensorId,))
                thread.start()
                threads.append(thread)
                success = True

    for index in range(int(pollutionSensors)):
        success = False
        while not success:
            sensorId = random.randint(1, 999)
            if sensorId not in randomList:
                randomList.append(sensorId)
                thread = threading.Thread(
                    target=sensor_producer.sendPollutionData,
                    args=(sensorId,))
                thread.start()
                threads.append(thread)
                success = True
    time.sleep(2)

    processes = []

    for index in range(int(terminals)):
        process = multiprocessing.Process(target=terminal.send_resultsServicer().run_server, args=(index + 1,))
        process.start()
        processes.append(process)

    process = multiprocessing.Process(target=proxy.run_client)
    process.start()
    processes.append(process)

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        pass

    for thread in threads:
        thread.join()
    for server in servers:
        server.stop(0)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
