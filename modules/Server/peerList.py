# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Package for handling a list of objects of the same type as a given one."""

import threading
import copy
import logging
from Common import orb

logging.basicConfig(format="%(levelname)s:%(filename)s: %(message)s",
                    level=logging.INFO)

class PeerList(object):

    """Class that builds a list of objects of the same type as this one."""

    def __init__(self, owner):
        self.owner = owner
        self.lock = threading.Condition()
        self.peers = {} # ID -> STUB

    # Public methods

    def initialize(self):
        """Populates the list of existing peers and registers the current
        peer at each of the discovered peers.

        It only adds the peers with lower ids than this one or else
        deadlocks may occur. This method must be called after the owner
        object has been registered with the name service.

        """

        # This isn't just some dumb function,
        # This should actually handle contacting the nameserver
        # And receiving the list of peers from it.

        try:
            self.lock.acquire()
            # Get the owner's access to the name server
            name_service = self.owner.name_service

            # Get the list of peers from the name service
            # We get a list of tuples from the name service
            # of the format (id, addr)
            peer_set = name_service.get_peers(self.owner.type)
        except:
            self.lock.release()
            raise

        # We need to make sure we release the lock if things go well, too.
        self.lock.release()
        
        # Using the list of tuples, register the peers
        # Then register itself with each peer registered
        for peer_tuple in peer_set:
            peer_id, peer_addr = peer_tuple
            if peer_id != self.owner.id:
                self.register_peer(peer_id, peer_addr,
                # We're just spawning, we don't need to check if they've died
                                   False) 
                self.peers[peer_id].register_peer(self.owner.id, self.owner.address)

    def destroy(self):
        """Unregister this peer from all others in the list."""
        # If we tell a dead peer to unregister us, we'll crash
        self.check_all_alive()

        self.lock.acquire()
        try:
            # Ask all the other peers to deregister us
            for fellowPeer in self.peers.keys():
                self.peers[fellowPeer].unregister_peer(self.owner.id)
        finally:
            self.lock.release()

    def register_peer(self, pid, paddr, doubleChecking=True):
        """Register a new peer joining the network."""
        if doubleChecking:
            self.check_all_alive()

        # Synchronize access to the peer list as several peers might call
        # this method in parallel.
        self.lock.acquire()
        try:
            self.peers[pid] = orb.Stub(paddr)
        finally:
            self.lock.release()

    def unregister_peer(self, pid):
        """Unregister a peer leaving the network."""
        # Synchronize access to the peer list as several peers might call
        # this method in parallel.

        self.lock.acquire()
        try:
            if pid in self.peers:
                del self.peers[pid]
                logging.info("The connection to Peer {} was closed.".format(pid))
            else:
                raise Exception("No peer with id: '{}'".format(pid))
        finally:
            self.lock.release()

        # Ought to be up-to-date.
        self.check_all_alive()

    def display_peers(self):
        """Display all the peers in the list."""

        self.lock.acquire()
        try:
            pids = sorted(self.peers.keys())
            print("List of peers of type '{}':".format(self.owner.type))
            for pid in pids:
                addr = self.peers[pid].address
                print("    id: {:>2}, address: {}".format(pid, addr))
        finally:
            self.lock.release()

    def get_peer(self, pid):
        """Return the object with the given id."""

        self.lock.acquire()
        try:
            return self.peers[pid]
        finally:
            self.lock.release()

    def get_peers(self):
        """Return all registered objects."""

        self.lock.acquire()
        try:
            return self.peers
        finally:
            self.lock.release()

    def check_alive(self, pID):
        """ Checks whether a peer has disconnected without telling us. """
        logging.debug("PeerList confirming connections to Peer {}.".format(pID))
        alive = orb.checkLiveness(pID, self.get_peer(pID), self.owner.type)
        if not alive:
            logging.debug("Confirmed {} is not alive.".format(pID))
            self.owner.unregister_peer(pID)

    def check_all_alive(self):
        """ Checks whether any peer has disconnected without telling us. """
        logging.debug("PeerList confirming connections to all peers.")
        allPeers = self.get_peers()
        for pID in list(allPeers.keys()):
            alive = orb.checkLiveness(pID, allPeers[pID], self.owner.type)
            if not alive:
                logging.debug("Confirmed {} is not alive.".format(pID))
                self.owner.unregister_peer(pID)
