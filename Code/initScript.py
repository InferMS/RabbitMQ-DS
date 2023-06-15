import getopt
import pickle
import random
import sys
import threading
import time
import multiprocessing
import grpc
from concurrent import futures
import redis

import grpc_sensor
import grpc_server
import grpc_proxy
import grpc_terminal
import grpc_LoadBalancerServer

import loadBalancerServer_pb2_grpc


def main():
    pollutionSensors = 1
    qualitySensors = 1
    servers_num = 2
    terminals = 2

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
    processes = []

    servers = []
    for index in range(int(servers_num)):
        thread = threading.Thread(
            target=grpc_server.ServerServicer.start,
            args=(index,))
        thread.start()
        threads.append(thread)

    thread = threading.Thread(
        target=grpc_LoadBalancerServer.LoadBalancerServicer.start,
        args=(servers_num,))
    thread.start()
    threads.append(thread)

    randomList = []
    clients = []

    for index in range(int(qualitySensors)):
        success = False
        while not success:
            sensorId = random.randint(1, 999)
            if sensorId not in randomList:
                randomList.append(sensorId)
                thread = threading.Thread(
                    target=grpc_sensor.sendMeteoData,
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
                    target=grpc_sensor.sendPollutionData,
                    args=(sensorId,))
                thread.start()
                threads.append(thread)
                success = True
    time.sleep(2)

    for index in range(int(terminals)):
        process = multiprocessing.Process(target=grpc_terminal.send_resultsServicer.run_server,
                                          args=(terminals, servers_num, index + 1,))
        process.start()
        processes.append(process)

    thread = threading.Thread(
        target=grpc_proxy.run_client,
        args=(terminals, servers_num,))
    thread.start()
    threads.append(thread)

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        pass

    for thread in threads:
        thread.join()
    for server in servers:
        server.stop(0)
    for process in processes:
        process.terminate()
        process.join()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()