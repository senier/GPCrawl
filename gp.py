#!/usr/bin/env python

import sys
import requests
from lxml import html
import stem.control
import stem.descriptor.remote
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

        self.__proxies = {
            'http':  'socks5h://USER-%s-XXX:password@localhost:9050' % (exitnode),
            'https': 'socks5h://USER-%s-YYY:password@localhost:9050' % (exitnode)}

        self.__headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def exitnode(self):
        return self.__exitnode

    def request(self, url, sleep = True):

        while True:
            try:
                return self.__session.get(url, headers = self.__headers, proxies = self.__proxies, verify = True, timeout = 30.0)
            except KeyboardInterrupt: raise
            except requests.exceptions.Timeout:
                if not sleep: return
                time.sleep(120)
            except Exception as e:
                print("request: " + str(e))
                time.sleep(1)

class TorPool:

    def get_numthreads(self):
	return self.__num_threads

    def start(self, WorkerClass, StatisticsClass = None):

        self.__circuit_attached = threading.Event()
        self.__circuit_id = None
        self.__num_threads = 0

        self.__controller = stem.control.Controller.from_socket_file()
        self.__controller.authenticate()

        self.__controller.set_conf('SocksTimeout', '60')
        self.__controller.set_conf('CircuitBuildTimeout', '10')
        self.__controller.set_conf('CircuitIdleTimeout', '60')
        self.__controller.set_conf('CircuitStreamTimeout', '9999999')
        self.__controller.set_conf('NewCircuitPeriod', '9999999')
        self.__controller.set_conf('MaxClientCircuitsPending', '1')

        for circuit in self.__controller.get_circuits():
            try:
                self.__controller.close_circuit(circuit.id)
            except: pass

        for stream in self.__controller.get_streams():
            try:
                self.__controller.close_stream(stream)
            except: pass

        def attach_stream(stream):
            if stream.status == 'NEW':
                if not self.__circuit_id:
                    return
                try:
                    self.__controller.attach_stream(stream.id, self.__circuit_id)
                except Exception as e:
                    print("Attach stream: " + str(e))
                finally:
                    self.__circuit_id = None
                    self.__circuit_attached.set()

        threads = []

        self.__server_descriptors = stem.descriptor.remote.get_server_descriptors()

        guard_nodes = [desc for desc in self.__server_descriptors if not desc.exit_policy.is_exiting_allowed()]
        guard_nodes.sort(key=lambda desc: desc.observed_bandwidth, reverse=True)

        exit_nodes = [desc for desc in self.__server_descriptors if desc.exit_policy.is_exiting_allowed()]
        exit_nodes.sort(key=lambda desc: desc.observed_bandwidth, reverse=True)

        print("%d exit nodes found" % (len(exit_nodes)))

        # Start statistics thread
        if StatisticsClass:
            stats = StatisticsClass(self)
            stats.daemon = True
            stats.start()

        try:
            self.__controller.add_event_listener(attach_stream, stem.control.EventType.STREAM)
            self.__controller.set_conf('__LeaveStreamsUnattached', '1')  # leave stream management to us

            for exit_node in exit_nodes:

                entry_node = random.choice(guard_nodes[1:50])

                print("%s: Attaching circuit" % (exit_node.fingerprint))
                self.__circuit_attached.clear()

                try:
                    self.__circuit_id = self.__controller.new_circuit([entry_node.fingerprint, exit_node.fingerprint], await_build = True)

                except KeyboardInterrupt: raise
                except Exception as e:
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

                # FIXME: Make maximum number of threads configurable
                if self.__num_threads > 330: break

            for thread in threads:
                thread.join()

        except KeyboardInterrupt: raise
        except Exception as e:
            print("start: " + str(e))
        finally:
            self.__controller.remove_event_listener(attach_stream)
            self.__controller.reset_conf('__LeaveStreamsUnattached')

    def prepare_write(self):
        pass

    def finish_write(self):
        pass

if __name__ == '__main__':
    while True:
        main()
