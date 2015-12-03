# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

import multiprocessing.dummy as multiprocessing
import sys
import threading
import socket
import json
import logging
import traceback
from json import JSONDecodeError

"""Object Request Broker

This module implements the infrastructure needed to transparently create
objects that communicate via networks. This infrastructure consists of:

--  Stub ::
        Represents the image of a remote object on the local machine.
        Used to connect to remote objects. Also called Proxy.
--  Skeleton ::
        Used to listen to incoming connections and forward them to the
        main object.
--  Peer ::
        Class that implements basic bidirectional (Stub/Skeleton)
        communication. Any object wishing to transparently interact with
        remote objects should extend this class.
"""

log = logging # Pretty sure I can remove this

class ComunicationError(Exception):
    pass

class ProtocolError(Exception):
    pass

class ExternalError(Exception):
    pass

def throw_ExternalError(error):
    logging.debug("ExternalError details:\n{}".format(error))

    errorFields = error["error"]
        
    raise ExternalError("An error occured on a different machine in the network." +\
                        "\n{}: {}".format(errorFields['name'],errorFields["args"][0]))

def handle_JSONDecodeError(err):
    if err.doc == "":
        raise ProtocolError("Received empty JSON!")
    message = """
    The JSON received was unable to be decoded!
    The error "{}" was raised at line {}, column {}, position {}
    in the following JSON:
    {}"""
    raise ProtocolError(
        message.format(err.msg, err.lineno, err.colno, err.pos, err.doc))
    

def json_dumps_method(method_name, args=[]):
    return json.dumps({"method": method_name, "args": args})    

def json_dumps_result(result):
    return json.dumps({"result": result})

def json_dumps_error(error):
    return json.dumps({"error": {"name": error.__class__.__name__, "args": error.args}})

class Request(threading.Thread):
    """Run the incoming requests on the owner object of the skeleton."""

    def __init__(self, owner, conn, addr):
        threading.Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.owner = owner
        self.daemon = True
        
    def process_request(self, request):
        try:
            r = json.loads(request)
            if ("method" not in r.keys() or "args" not in r.keys()):
                raise ProtocolError("Bad stuff")
            method = r["method"]
            args = r["args"]
            result = getattr(self.owner, method).__call__(*args)
            return json_dumps_result(result)
        except OSError as detail:
            logging.info(traceback.format_exc())
            return json_dumps_error(detail)
        except JSONDecodeError as err:
            handle_JSONDecodeError(err)

    def run(self):
        try:
            # Treat the socket as a file stream.
            worker = self.conn.makefile(mode="rw")
            # Read the request in a serialized form (JSON).
            request = worker.readline()
            logging.debug("Request received: {}".format(request))
            # Process the request.
            result = self.process_request(request)
            logging.debug("Request processed. Sending result {}\n".format(result))
            # Send the result.
            worker.write(str(result) + '\n')
            worker.flush()
        finally:
            self.conn.close()


class Stub(object):
    """ Stub for generic objects distributed over the network.

    This is a wrapper object for a socket.
    """

    def __init__(self, address):
        logging.debug("Stub.__init__()")
        self.address = tuple(address)

    def _rmi(self, method, *args):
        logging.debug("Stub._rmi({}, {})".format(method, args))
        try:
            conn = socket.create_connection(self.address)
            # Treat the socket as a file stream.
            worker = conn.makefile(mode="rw")
            msg = json_dumps_method(method, args)
            logging.debug("Stub sending JSON message: {}".format(msg))
            worker.write(msg + '\n')
            worker.flush()
            # Read the request in a serialized form (JSON).
            answer = worker.readline()
            logging.debug(answer)
            # Process the request.
            response = json.loads(answer)
            
            if (set(response.keys()) != set(["error"]) and set(response.keys()) != set(["result"])):
                raise ProtocolError("Bad key(s):", response)
            if ("error" in response.keys()):
                throw_ExternalError(response)
            
            result = response["result"]
            return result
        except JSONDecodeError as err:
            conn.close()
            handle_JSONDecodeError(err)

    def __getattr__(self, attr):
        """Forward call to name over the network at the given address."""
        logging.debug("Stub.__getattr__({})".format(attr))

        def rmi_call(*args):
            return self._rmi(attr, *args)
        return rmi_call

class Skeleton(threading.Thread):
    """ Skeleton class for a generic owner.

    This is used to listen to an address of the network, manage incoming
    connections and forward calls to the generic owner class.
    """

    def __init__(self, owner, address):
        logging.debug("Skeleton.__init__()")
        threading.Thread.__init__(self)
        self.address = address
        self.owner = owner
        self.daemon = True

    def run(self):
        logging.debug("Skeleton.run()")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(self.address)
        listener.listen(1)
        logging.debug("Skeleton running at: {}".format(self.address))
        logging.info("Press Ctrl-C to stop the peer...")
        try:
            while True:
                try:
                    conn, addr = listener.accept()
                    req = Request(self.owner, conn, addr)
                    logging.debug("Serving a request from {0}".format(addr))
                    req.start()
                except socket.error as socket_error:
                    logging.debug(socket_error)
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            listener.close()


class Peer(object):
    """Class, extended by objects that communicate over the network."""

    def __init__(self, l_address, ns_address, ptype):
        logging.debug("Peer.__init__()")
        self.type = ptype
        self.hash = ""
        self.id = -1
        self.address = self._get_external_interface(l_address)
        self.skeleton = Skeleton(self, self.address)
        self.name_service_address = self._get_external_interface(ns_address)
        self.name_service = Stub(self.name_service_address)

    # Private methods

    def _get_external_interface(self, address):
        """ Determine the external interface associated with a host name.

        This function translates the machine's host name into the
        machine's external address, not into '127.0.0.1'.
        """
        logging.debug("Peer._get_external_interface(self, {})".format(address))

        addr_name = address[0]
        if addr_name != "":
            addrs = socket.gethostbyname_ex(addr_name)[2]
            if len(addrs) == 0:
                raise ComunicationError("Invalid address to listen to")
            elif len(addrs) == 1:
                addr_name = addrs[0]
            else:
                al = [a for a in addrs if a != "127.0.0.1"]
                addr_name = al[0]
        addr = list(address)
        addr[0] = addr_name
        return tuple(addr)

    # Public methods

    def start(self):
        """Start the communication interface."""
        
        logging.debug("Peer starting...")
        logging.debug("Peer starting Skeleton...")
        self.skeleton.start()

        logging.debug("Peer done starting Skeleton!")
        logging.debug("Peer registering name service...")
        self.id, self.hash = self.name_service.register(self.type,
                                                        self.address)
        logging.debug("Peer done registering name service!\n{}"
                      .format((self.id, self.hash)))

    def destroy(self):
        """Unregister the object before removal."""

        logging.debug("Peer unregistering from name service...")
        self.name_service.unregister(self.id, self.type, self.hash)
        logging.debug("Peer unregistered from name service")

    def isAlive(self):
        """Someone is checking to see if I'm still alive."""
        logging.debug("Someone wants to know I'm still alive. Responding with {}".format((self.id, self.type)))
        return (self.id, self.type)

# Liveness Checking
# (Really doesn't need to belong to any object in particular)

def checkLiveness(pID, pStub, obj_type, timeout=5):
    """ Used to detect if a peer is still alive. """
    logging.debug("Confirming connection to peer {}.".format(pID))
    try:
        parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
        p = multiprocessing.Process(
            target=_get_line,
            args=(child_conn, pID, pStub, obj_type)
        )
        p.daemon = True
        p.start()
        it_did_timeout = (not parent_conn.poll(timeout))
        if it_did_timeout:
            logging.info("Connection to peer {} timed out.".format(pID))
            return False
        else:
            parent_said_yes = parent_conn.recv()
            if not parent_said_yes:
                logging.info("No connection to peer {} established.".format(pID))
            return parent_said_yes
    except:
        err = sys.exc_info()
        logging.debug("Encountered an error while spawning a process to check if peer {} is still alive:\n{}: {}"
                      .format(pID, err[0], err[1]))
        return False

def _get_line(conn, pID, pStub, obj_type):
    """ Helper function for checkLiveness(). """
    result = False
    try:
        expected = [pID, obj_type]
        response = pStub.isAlive()
        result = (response == expected)
        logging.debug("PeerList received response {} from peer {}".format(response, pID))
        if result is True:
            logging.debug("This was the expected response.")
        else:
            logging.debug("This was not the expected response.\n Expected response: {}"
                          .format(expected))
    except ConnectionRefusedError:
        logging.info("Peer {} refused connection".format(pID))
        result = False
    except:
        err = sys.exc_info()
        logging.debug("Encountered an error trying to check if peer {} is still alive:\n{}: {}"
                     .format(pID, err[0], err[1]))
    finally:
        conn.send(result)

