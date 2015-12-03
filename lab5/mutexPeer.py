#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""This is an implementation of mutual exclusion among a list of peers."""

import sys
import random
import socket
import argparse

sys.path.append("../modules")
from Common import orb
from Common.nameServiceLocation import name_service_address
from Common.objectType import object_type

from Server.peerList import PeerList
from Server.Lock.distributedLock import DistributedLock

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------

class Client(orb.Peer):

    """Distributed mutual exclusion client class."""

    def __init__(self, local_address, ns_address, client_type):
        """Initialize the client."""
        orb.Peer.__init__(self, local_address, ns_address, client_type)
        self.peer_list = PeerList(self)
        self.distributed_lock = DistributedLock(self, self.peer_list)
        self.dispatched_calls = {
            "display_peers":      self.peer_list.display_peers,
            "acquire":            self.distributed_lock.acquire,
            "release":            self.distributed_lock.release,
            "request_token":      self.distributed_lock.request_token,
            "obtain_token":       self.distributed_lock.obtain_token,
            "display_status":     self.distributed_lock.display_status
        }
        orb.Peer.start(self)
        self.peer_list.initialize()
        self.distributed_lock.initialize()

    # Public methods

    def destroy(self):
        # Destroy the lock first to allow the token to be passed if we have it
        self.distributed_lock.destroy()
        orb.Peer.destroy(self)
        self.peer_list.destroy()

    def __getattr__(self, attr):
        """Forward calls are dispatched here."""
        if attr in self.dispatched_calls:
            return self.dispatched_calls[attr]
        else:
            raise AttributeError(
                "Client instance has no attribute '{}'".format(attr))

    def register_peer(self, pid, paddr):
        self.peer_list.register_peer(pid, paddr)
        self.distributed_lock.register_peer(pid)

    def unregister_peer(self, pid):
        self.peer_list.unregister_peer(pid)
        self.distributed_lock.unregister_peer(pid)


# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

def main():
    rand = random.Random()
    rand.seed()
    description = """Peer with access to a mutual exclusive component."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-p", "--port", metavar="PORT", dest="port", type=int,
        default=rand.randint(1, 10000) + 40000, choices=range(40001, 50000),
        help="Set the port to listen to. Must be in the range 40001 .. 50000. "
             "The default value is chosen at random."
    )
    parser.add_argument(
        "-t", "--type", metavar="TYPE", dest="type", default=object_type,
        help="Set the type of the client."
    )
    opts = parser.parse_args()

    local_port = opts.port
    client_type = opts.type
    assert client_type != "object", "Change the object type to something unique!"

    # Initialize the client object.
    local_address = (socket.gethostname(), local_port)
    p = Client(local_address, name_service_address, client_type)


# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

    command = ""
    cursor = "{}({}):{}> ".format(p.type, p.id, displayState(p))
    menu()
    while command != "q":
        try:
            cursor = "{}({}):{}> ".format(p.type, p.id, displayState(p))
            sys.stdout.write(cursor)
            command = input()
            if command == "l":
                p.display_peers()
            elif command == "s":
                p.display_status()
            elif command == "a":
                p.acquire()
            elif command == "r":
                p.release()
            elif command == "h":
                menu()
        except KeyboardInterrupt:
            break
        except Exception as e:
            # Catch all errors to keep on running in spite of all errors.
            print("An error has occurred: {}.".format(e))

    # Kill our peer object.
    p.destroy()

def displayState(peer):
    #NO_TOKEN = 0
    #TOKEN_PRESENT = 1
    #TOKEN_HELD = 2
    state = peer.distributed_lock.get_state()
    if state == 0:
        return("NO_TOKEN")
    elif state == 1:
        return("TOKEN_PRESENT")
    elif state == 2:
        return("TOKEN_WITHHELD")
    else:
        return("BUT WHERE'S THE CAVEMAN? (SOMETHING WENT WRONG)")

def menu():
    print("""\
Choose one of the following commands:
    l  ::  list peers,
    s  ::  display status,
    a  ::  acquire the lock,
    r  ::  release the lock,
    h  ::  print this menu,
    q  ::  exit.\
""")

if __name__ == "__main__": main()
