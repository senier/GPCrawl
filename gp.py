#!/usr/bin/env python

import sys
import requests
from lxml import html
import stem.control
import threading
import time
import random

class Worker(threading.Thread):

    def __init__(self, exitnode, pool):

        # Initialize threading
        threading.Thread.__init__(self)

        self.__session  = requests.Session()
        self.__exitnode = exitnode
        self.__pool     = pool

        self.__session.proxies = {
            'http':  'socks5h://USER-%s-XXX:password@localhost:9050' % (exitnode),
            'https': 'socks5h://USER-%s-YYY:password@localhost:9050' % (exitnode)}

        self.__headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def exitnode(self):
        return self.__exitnode

    def request(self, url):

        while True:
            try:
                return self.__session.get(url, headers = self.__headers, verify = True, timeout = 5.0)
            except KeyboardInterrupt: raise
            except requests.exceptions.Timeout: return None
            except Exception as e:
                print(str(e))
                time.sleep(1)

class TorPool:

    def get_numthreads(self):
	return self.__num_threads

    def start(self, WorkerClass, StatisticsClass = None):

        self.__circuit_attached = threading.Event()
        self.__circuit_id = None
        self.__num_threads = 0

        controller = stem.control.Controller.from_socket_file()
        controller.authenticate()

        for circuit in controller.get_circuits():
            controller.close_circuit(circuit.id)

        for stream in controller.get_streams():
            controller.close_stream(stream)

        def attach_stream(stream):
            if stream.status == 'NEW':
                if not self.__circuit_id:
                    return
                try:
                    controller.attach_stream(stream.id, self.__circuit_id)
                finally:
                    self.__circuit_id = None
                    self.__circuit_attached.set()

        threads = []
        exit_nodes = [desc for desc in controller.get_server_descriptors() if desc.exit_policy.is_exiting_allowed()]
        exit_nodes.sort(key=lambda desc: desc.observed_bandwidth, reverse=True)

        print("%d exit nodes found" % (len(exit_nodes)))

        # Start statistics thread
        if StatisticsClass:
            stats = StatisticsClass(self)
            stats.daemon = True
            stats.start()

        try:
            controller.add_event_listener(attach_stream, stem.control.EventType.STREAM)
            controller.set_conf('__LeaveStreamsUnattached', '1')  # leave stream management to us

            for exit_node in exit_nodes:

                while True:
                    entry_node = random.choice(exit_nodes[1:50])
                    if entry_node.fingerprint != exit_node.fingerprint: break

                print("%s: Attaching circuit" % (exit_node.fingerprint))
                self.__circuit_attached.clear()

                try:
                    self.__circuit_id = controller.new_circuit([entry_node.fingerprint, exit_node.fingerprint], await_build = True)

                except stem.CircuitExtensionFailed as e:
                    print("%s: !!! Creation failed: %s" % (exit_node.fingerprint, str(e)))
                    continue

                print("%s: Starting" % (exit_node.fingerprint))
                thread = WorkerClass(exit_node, self)
                thread.daemon = True
                threads.append (thread)

                # Wait for circuit to be attached
                if not self.__circuit_attached.wait(10.0):
                    print("Attaching circuit timed out")
                    continue

                thread.start()
                self.__num_threads += 1

            for thread in threads:
                thread.join()

        except KeyboardInterrupt: raise
        except Exception as e:
            print(str(e))
        finally:
            controller.remove_event_listener(attach_stream)
            controller.reset_conf('__LeaveStreamsUnattached')

if __name__ == '__main__':
    main()
