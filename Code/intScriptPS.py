import sys
import getopt
import terminal
import proxy
import matplotlib
matplotlib.use('TkAgg')
import multiprocessing
import time
import matplotlib.pyplot as plt

from terminal_service import terminal_service

def main():
    servers_num = 2
    terminals = 2

    argv = sys.argv[1:]

    opts, args = getopt.getopt(argv, "s:t:",
                               ["servers=",
                                "terminals="])

    for opt, arg in opts:
        if opt in ['-s', '--servers']:
            servers_num = arg
        elif opt in ['-t', '--terminals']:
            terminals = arg

    processes = []

    for index in range(int(terminals)):
        process = multiprocessing.Process(target=terminal.send_resultsServicer(index + 1).run_server, args=(index + 1,))
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

    for process in processes:
        process.terminate()
        process.join()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
