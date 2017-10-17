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
                return self.__session.get(url, headers = self.__headers, verify = True)
            except KeyboardInterrupt: raise
            except Exception as e:
                print(str(e))
                time.sleep(1)

class TorPool:

    def start(self, WorkerClass):

        self.circuit_id = None

        controller = stem.control.Controller.from_socket_file()
        controller.authenticate()

        for circuit in controller.get_circuits():
            controller.close_circuit(circuit.id)

        for stream in controller.get_streams():
            controller.close_stream(stream)

        def attach_stream(stream):

            print("attach_stream circ=%s, stream=%s: " % (str(self.circuit_id), str(stream)))
            if stream.status == 'NEW':
                controller.attach_stream(stream.id, self.circuit_id)
                self.circuit_id = None

        threads = []
        exit_nodes = [(desc.fingerprint, desc.observed_bandwidth) for desc in controller.get_server_descriptors() if desc.exit_policy.is_exiting_allowed()]
        exit_nodes.sort(key=lambda tup: tup[1], reverse=True)

        print("%d exit nodes found" % (len(exit_nodes)))

        try:
            controller.add_event_listener(attach_stream, stem.control.EventType.STREAM)
            controller.set_conf('__LeaveStreamsUnattached', '1')  # leave stream management to us

            for (exit_node, unused) in exit_nodes:

                while True:
                    entry_node = random.choice(exit_nodes[1:50])[0]
                    if entry_node != exit_node: break

                try:
                    self.circuit_id = controller.new_circuit([entry_node, exit_node], await_build = True)
                except stem.CircuitExtensionFailed as e:
                    print("%s: !!! Creation failed: %s" % (exit_node, str(e)))
                    continue

                print("%s: Starting" % (exit_node))
                thread = WorkerClass(exit_node, self)
                thread.deamon = True
                thread.start()
                threads.append (thread)

            for thread in threads:
                thread.join()

        finally:
            controller.remove_event_listener(attach_stream)
            controller.reset_conf('__LeaveStreamsUnattached')

if __name__ == '__main__':
    main()
