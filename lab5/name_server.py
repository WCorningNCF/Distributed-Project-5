'''
Created on Sep 28, 2015

@author: jackie
'''

import multiprocessing.dummy as multiprocessing
import copy
import socket
import logging
import sys
sys.path.append("../modules")
from Common import nameServiceLocation
from Common.orb import Request
from Common.orb import Stub
from Common.orb import ProtocolError
from Common.readWriteLock import ReadWriteLock

import random

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

description = """\
Name server for a group of peers. It allows peers to find each other by object_id.\
"""

server_address = nameServiceLocation.name_service_address

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------

logging.basicConfig(format="%(levelname)s:%(filename)s: %(message)s", level=logging.INFO)

class NameServer(object):
    """Class that handles peers."""

    # The dictionary self.peers assigns a set to each object type
    # This set contains tuples that represent the peers of that type
    # The tuple is of the id of that peer and their address (id, addr)

    # So for example, self.peers could look like this:

    # obj_type (key)    | peers (entry)
    # ------------------+------------------------------------------------------
    # [obj0]            | [ (0, addr0), (3, addr3) ]
    # [obj1]            | [ (1, addr1), (4, addr4), (5, addr5) ]
    # [obj2]            | [ (2, addr2) ]

    def __init__(self):
        self.lock = ReadWriteLock()
        self.peers = dict()         # Contains a set of peers for each object type
        self.responses = dict()
        self.next_id = 0
        self.rand = random.Random()
        self.rand.seed()

    # Public methods

    def register(self, obj_type, address):
        self._check_all_alive(obj_type) # Make sure everyone in our group is still alive

        address = tuple(address)    # The address might come in as a list.
        logging.debug("NameServer registering peer at {}".format(address))

        # We're making modifications to the NameServer's data
        self.lock.write_acquire()
        obj_hash = address       # Set the hash to the address (for now)
        obj_id = self.next_id
        self.next_id += 1
        t = (obj_id, obj_hash)
        self.lock.write_release()

        # We're adding the address to the group
        group = self._get_group(obj_type)
        self.lock.write_acquire() # Acquire the lock before we add ourselves to the group
        group.add(t)              # Add ourselves
        self.lock.write_release() # Release the lock

        logging.info("NameServer done registering peer at {}".format(address))
        return t

    def unregister(self, obj_id, obj_type, obj_hash):
        logging.debug("NameServer unregistering peer at {}".format(tuple(obj_hash)))

        # Get the data necessary
        group = self._get_group(obj_type)
        t = (obj_id, tuple(obj_hash))

        # Remove from the group (if it exists)
        self.lock.write_acquire()
        if t in group:
            group.remove(t)
        else:
            logging.debug("\nERR: Unregistering peer not registered!\n{}"
                          .format((obj_id,obj_type,obj_hash)))
        self.lock.write_release()
        logging.info("NameServer done unregistering peer at {}".format(tuple(obj_hash)))
	# This function doesn't stop until every peer has been checked.
        # BUT there's a peer out there waiting for this function to return
        # Before it can be unregistered.
        # This is very obnoxious.
        self._check_all_alive(obj_type) # Make sure everyone in our group is still alive
        return "null"

    def get_peers(self, obj_type):
        return list(self._get_group(obj_type))

    def require_any(self, obj_type):
        all_matching_peers = self.get_peers(obj_type)
        return all_matching_peers[self.rand.randint(0,len(p))][1] # Return only the address

    def require_object(self, server_type, server_id):
        server_type_list = self.get_peers(server_type)
        return server_type_list[server_id]
        
    def _get_group(self, obj_type):
        # Create our group if it doesn't already exist
        self.lock.write_acquire()
        if obj_type not in self.peers.keys():
            self.peers[obj_type] = set()
        self.lock.write_release()

        # Fetch and return the group
        self.lock.read_acquire()
        group = self.peers.get(obj_type)
        self.lock.read_release()
        return group

    def _check_all_alive(self, obj_type):
        logging.info("NameServer confirming connections to all peers" \
                 + " of type {}.".format(obj_type))
        group = copy.deepcopy(self._get_group(obj_type))
        for peer in group:
            self._check_alive(obj_type, peer)

    def _check_alive(self, obj_type, peer):
        logging.debug("NameServer confirming connection to peer {}.".format(peer[0]))
        if not self._is_alive(obj_type, peer, 5):
            t = peer
            group = self._get_group(obj_type)
            self.lock.write_acquire()
            logging.info("Removing peer {}.".format(t))
            group.remove(t)
            self.lock.write_release()

    def _is_alive(self, obj_type, peer, timeout=5):
        try:
            parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
            p = multiprocessing.Process(
                target=self._get_line,
                args=(child_conn, peer, obj_type)
            )
            p.daemon = True
            p.start()
            it_did_timeout = (not parent_conn.poll(timeout))
            if it_did_timeout:
                logging.info("Connection to peer {} timed out.".format(peer))
                return False
            else:
                parent_said_yes = parent_conn.recv()
                if not parent_said_yes:
                    logging.info("No connection to peer {} established.".format(peer))
                return parent_said_yes
        except:
            err = sys.exc_info()
            logging.debug("NameServer encountered an error while spawning a process to check if peer {} is still alive:\n{}: {}"
                          .format(peer, err[0], err[1]))
            return False

    def _get_line(self, conn, peer, obj_type):
        result = False
        try:
            expected = [peer[0], obj_type]
            response = Stub(peer[1]).isAlive()
            result = (response == expected)
            logging.debug("NameServer received response {} from peer {}".format(response, peer))
            if result is True:
                logging.debug("This was the expected response.")
            else:
                logging.debug("This was not the expected response.\n Expected response: {}"
                              .format(expected))
        except ConnectionRefusedError:
            logging.info("Peer {} refused connection".format(peer))
            result = False
        except:
            err = sys.exc_info()
            logging.debug("NameServer encountered an error trying to check if peer {} is still alive:\n{}: {}"
                         .format(peer, err[0], err[1]))
        finally:
            conn.send(result)

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    logging.info("NameServer listening to: {}:{}".format(server_address[0], server_address[1]))

    nameserver = NameServer()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("", server_address[1]))
    listener.listen(1)

    logging.info("Press Ctrl-C to stop the name server...")

    try:
        while True:
            conn, addr = listener.accept()
            req = Request(nameserver, conn, addr)
            logging.debug("NameServer serving a request from {}".format(addr))
            req.start()
            logging.debug("NameServer served the request from {}".format(addr))
    except KeyboardInterrupt:
        pass
    finally:
        listener.close()
        logging.info("NameServer has been unbound")
