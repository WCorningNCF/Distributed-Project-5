# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Rikard-Agravara algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictinaries: request, token,
        and peer_list) should be protected.
    --  the implementation should gratiously handle situations when a
        peer dies unexpectedly. All exceptions comming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated acordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.

"""

from threading import Lock
import time
from collections import Counter

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.state = NO_TOKEN
        self.localLock = Lock()

        # WARNING:
        # DO NOT DEPEND ON THESE COUNTERS FOR LOOPING, instead
        # use self.peer_list to get a full list of connected peers.
        self.token = Counter()
        self.request = Counter()
        # You cannot loop through their keys and expect them to contain
        # each and every peer. As counters, it is the case that there
        # could be some peers whose values are 0 and are NOT explicitly
        # listed in the counter. It is also the case that there could be
        # some peers whose values are 0 and ARE explicitly listed.
        # Do not ever depend on one case or the other.

    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.

        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return Counter(token)

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """

        # If I don't have any peers, spawn with a token!
        peerIDs = self.peer_list.get_peers()
        if len(peerIDs) == 0:
            self.state = TOKEN_PRESENT

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        self._offload_token()

    def register_peer(self, pID):
        """Called when a new peer joins the system."""
        # We don't need to do anything because our token and requests
        # are both counters, so their values are alreadt at 0 implicitly
        pass

    def unregister_peer(self, pID):
        """Called when a peer leaves the system."""
        # We don't need to delete from the token,
        # that's _clean_token's job
        # PLUS, it's possible the peerlists on either side could change
        # while the token is in-flight.
        del self.request[pID]

    """
        Acquisition scheme:
            Request token from all registered peers.
                In this RMI framework, peers will respond with an acknowledgement
                message regardless of whether they have the token.
                    If no ack is received within a timeout period, there should be
                    some exception handling.
            Wait for a peer to send the token using our obtain_token() method.
    """

    def acquire(self):
        """Called when this object tries to acquire the lock."""

        # If we don't have the token, ask everyone for it
        if self.state == NO_TOKEN:
            # Increment our local timer
            self.time+=1

            self.request[self.owner.id]=self.time

            for peer in self.peer_list.get_peers().values():
                peer.request_token(self.time,self.owner.id)

            # If we acquired the token while requesting, this will pass immediately
            print("Status is {}. Waiting for token...".format(self.state))
            while self.state == NO_TOKEN:
                time.sleep(1)

        # If we do have the token, we can just silently acquire it
        # If we have it but someone else asked for it, this should be
        # taken care of already; no one should have the opportunity to
        # decide to use their token when they know that someone else has
        # already asked them for it.
        elif self.state == TOKEN_PRESENT:
            self.state = TOKEN_HELD
        else:
            print("I've already locked the token!")


    def release(self):
        """Called when this object releases the lock."""
        
        if self.state is TOKEN_HELD:
            self.state = TOKEN_PRESENT

            # Safely initiate token transfer if possible
            self._check_token()
        else:
            print("Warning: release() called when lock not in state TOKEN_HELD")


    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""

        # Update this client's last-requested timestamp for the other client
        # We want the max timestamp in case messages are somehow sent out-of-order.
        self.request[pid] = max(self.request[pid], time)

        if self.state == TOKEN_PRESENT and self.token[pid] < self.request[pid]:
            # Safely initiate token transfer

            self._check_token()
            
            # Note that we are not necessarily going to send the token to the peer
            # that just requested it.

        return "{} acknowledging request from {}".format(self.owner.id,pid)


    def obtain_token(self, token):
        """Called when some other object is giving us the token."""

        if self.state is not NO_TOKEN:
            print("WARNING: peer {} has received a token when it already had one".format(self.owner.id))
        
        # Convert all peer IDs in the token from string to int; cast to Counter
        self.token = Counter({int(k): token[k] for k in token})

        # Assume we're getting the token in response to our request
        # if we've asked for it since we last held it.
        tokenWasWanted = self.request[self.owner.id] > self.token[self.owner.id]

        # Update the token's last-held timestamp for this client
        self.token[self.owner.id] = self.time

        self.state = TOKEN_HELD

        if not tokenWasWanted:
            self.release()

    def _clean_token(self):
        """Called when sending a token to clear out old records from peers that have disconnected"""
        self.peer_list.lock.acquire()
        all_peers = self.peer_list.get_peers()

        # Discard any unknown peer entries in the token
        self.token = Counter({pid: self.token[pid] for pid in self.token if pid == self.owner.id or pid in all_peers})
        self.peer_list.lock.release()

    def _check_token(self):
        """Called when this object checks its set of token requests in order
        to find a peer that should get the token"""

        # If we don't have the token or we are using it right now, we can just stop here
        if self.state is not TOKEN_PRESENT:
            print("WARNING: _check_token called when in not in state TOKEN_PRESENT")
            if self.state is NO_TOKEN: return False

        # Use Python's built-in locking (like Java's semaphore) to prevent
        # more than one of these checks from being run concurrently
        self.localLock.acquire()

        targetID = None
        wasSent = False

        requester_ids = self.request.keys()
        
        gt = sorted([pid for pid in requester_ids if pid > self.owner.id])
        lt = sorted([pid for pid in requester_ids if pid < self.owner.id])

        # Check each peer in clockwise order to see if anyone wants the token
        for pid in gt + lt:
            if self.request[pid] > self.token[pid]:
                targetID = pid
                break

        if targetID is not None:
            try:
                self._clean_token()
                self.state = NO_TOKEN
                self.peer_list.get_peers()[pid].obtain_token(self.token)
                wasSent = True
            except Exception as e:
                print("ERROR: Could not send token to pid",pid)
                print(e)

        self.localLock.release()

        return(wasSent)

    def _offload_token(self):
        """Called when this object needs to send its token to another peer immediately"""
        wasSent = False

        if self.state is not TOKEN_PRESENT:
            print("WARNING: _offload_token called when in not in state TOKEN_PRESENT")
            if self.state is NO_TOKEN: return False

        # First, try sending the token normally
        if self._check_token():
            print("Successfully sent token to a peer that had requested it")
            return True

        self.localLock.acquire()
        self._clean_token()

        # Try sending the token to everybody on our peer list
        all_peers = self.peer_list.get_peers()
        for pid in all_peers:
            try:
                self.state = NO_TOKEN
                all_peers[pid].obtain_token(self.token)
                wasSent = True
                break
            except Exception as e:
                print("ERROR: Could not forcibly send token to pid {}:".format(pid))
                print(e)

        self.localLock.release()

        return(wasSent)

    def display_status(self):
        """Print the status of this peer."""
        self.localLock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.localLock.release()

    def get_state(self):
        return self.state
