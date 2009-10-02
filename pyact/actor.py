"""
Copyright (c) 2009, Donovan Preston
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import sys
import traceback
import uuid
import weakref

import simplejson

from eventlet import api
from eventlet import coros

from pyact import exc
from pyact import shape


## If set to true, an Actor will print out every exception, even if the parent
## handles the exception properly.
NOISY_ACTORS = False


class ActorError(RuntimeError):
    """Base class for actor exceptions.
    """


class Killed(ActorError):
    """Exception which is raised when an Actor is killed.
    """
    pass


class DeadActor(ActorError):    
    """Exception which is raised when a message is sent to an Address which
    refers to an Actor which is no longer running.
    """
    pass


class RemoteAttributeError(ActorError, AttributeError):
    pass


class RemoteException(ActorError):
    pass


def is_actor_type(obj):
    """Return True if obj is a subclass of Actor, False if not.
    """
    try:
        return issubclass(obj, Actor)
    except TypeError:
        return False


def spawn(spawnable, *args, **kw):
    """Start a new Actor. If spawnable is a subclass of Actor,
    instantiate it with no arguments and call the Actor's "main"
    method with *args and **kw.

    If spawnable is a callable, call it inside a new Actor with the first
    argument being the "receive" method to use to retrieve messages out
    of the Actor's mailbox,  followed by the given *args and **kw.

    Return the Address of the new Actor.
    """
    if is_actor_type(spawnable):
        spawnable = spawnable()
    else:
        spawnable = Actor(spawnable)

    spawnable._args = (args, kw)
    api.call_after_global(0, spawnable.switch)
    return spawnable.address


def spawn_link(spawnable, *args, **kw):
    """Just like spawn, but call actor.add_link(api.getcurrent().address)
    before returning the newly-created actor.

    The currently running Actor will be linked to the new actor. If an
    exception occurs or the Actor finishes execution, a message will
    be sent to the Actor which called spawn_link with details.

    When an exception occurs, the message will have a pattern like:

        {'address': eventlet.actor.Address, 'exception': dict}

    The "exception" dict will have information from the stack trace extracted
    into a tree of simple Python objects.

    On a normal return from the Actor, the actor's return value is given
    in a message like:

        {'address': eventlet.actor.Address, 'exit': object}
    """
    if is_actor_type(spawnable):
        spawnable = spawnable()
    else:
        spawnable = Actor(spawnable)

    spawnable._args = (args, kw)
    spawnable.add_link(api.getcurrent().address)
    api.call_after_global(0, spawnable.switch)
    return spawnable.address


def handle_address(obj):
    if isinstance(obj, Address):
        return {'address': obj.actor_id}
    raise TypeError(obj)


def generate_address(obj):
    if obj.keys() == ['address']:
        return Actor.all_actors[obj['address']].address
    return obj


class Address(object):
    """An Address is a reference to another Actor.  Any Actor which has an
    Address can asynchronously put a message in that Actor's mailbox. This is
    called a "cast". To send a message to another Actor and wait for a response,
    use "call" instead.
    """
    def __init__(self, actor):
        self.__actor = weakref.ref(actor)

    @property
    def _actor(self):
        """This will be inaccessible to Python code in the C implementation.
        """
        actor = self.__actor()
        if actor is None:
            raise DeadActor()
        return actor

    @property
    def actor_id(self):
        return self._actor.actor_id

    def link(self, trap_exit=True):
        """Link the current Actor to the Actor at this address. If the linked
        Actor has an exception or exits, a message will be cast to the current
        Actor containing details about the exception or return result.
        """
        self._actor.add_link(api.getcurrent().address, trap_exit=trap_exit)

    def cast(self, message):
        """Send a message to the Actor this object addresses.
        """
        ## TODO: Copy message or come up with some way of "freezing" message
        ## so that actors do not share mutable state.
        self._actor._cast(simplejson.dumps(message, default=handle_address))

    def call(self, method, message, timeout=None):
        """Send a message to the Actor this object addresses.
        Wait for a result. If a timeout in seconds is passed, raise
        api.TimeoutError if no result is returned in less than the timeout.
        
        This could have nicer syntax somehow to make it look like an actual method call.
        """
        message_id = str(uuid.uuid1())
        my_address = api.getcurrent().address
        self._actor._cast(
            simplejson.dumps(
                {'call': message_id, 'method': method,
                'address': my_address, 'message': message},
            default=handle_address))
        if timeout is None:
            cancel = None
        else:
            ## Raise any TimeoutError to the caller so they can handle it
            cancel = api.exc_after(timeout, api.TimeoutError)

        RSP = {'response': message_id, 'message': object}
        EXC = {'response': message_id, 'exception': object}
        INV = {'response': message_id, 'invalid_method': str}

        pattern, response = api.getcurrent().receive(RSP, EXC, INV)

        if cancel is not None:
            cancel.cancel()

        if pattern is INV:
            raise RemoteAttributeError(method)
        elif pattern is EXC:
            raise RemoteException(response)
        return response['message']

    def wait(self):
        """Wait for the Actor at this Address to finish, and return it's result.
        """
        return self._actor._exit_event.wait()

    def kill(self):
        """Violently kill the Actor at this Address. Any other Actor which has
        called wait on this Address will get a Killed exception.
        """
        api.kill(self._actor, Killed)


CALL_PATTERN = {'call': str, 'method': str, 'address': Address, 'message': object}
RESPONSE_PATTERN = {'response': str, 'message': object}
INVALID_METHOD_PATTERN = {'response': str, 'invalid_method': str}


def lazy_property(property_name, property_factory, doc=None):
    def get(self):
        if not hasattr(self, property_name):
            setattr(self, property_name, property_factory(self))
        return getattr(self, property_name)
    return property(get)


class Actor(api.Greenlet):
    """An Actor is a Greenlet which has a mailbox.  Any other Actor which has
   the Address can asynchronously put messages in this mailbox.

    The Actor extracts messages from this mailbox using a technique called
    selective receive. To receive a message, the Actor calls self.receive,
    passing in any number of "shapes" to match against messages in the mailbox.
    
    A shape describes which messages will be extracted from the mailbox.
    For example, if the message ('credit', 250.0) is in the mailbox, it could
    be extracted by calling self.receive(('credit', int)). Shapes are Python
    object graphs containing only simple Python types such as tuple, list,
    dictionary, integer, and string, or type object constants for these types.
    
    Since multiple patterns may be passed to receive, the return value is
    (matched_pattern, message). To receive any message which is in the mailbox,
    simply call receive with no patterns.
    """
    _waiting = False
    _mailbox = lazy_property('_p_mailbox', lambda self: [])
    _links = lazy_property('_p_links', lambda self: [])
    _exit_links = lazy_property('_p_exit_links', lambda self: [])
    _exit_event = lazy_property('_p_exit_event', lambda self: coros.event())

    address = lazy_property('_p_address', lambda self: Address(self),
        doc="""An Address is a reference to another Actor. See the Address
        documentation for details. The address property is the Address
        of this Actor.
        """)

    spawn = classmethod(spawn)
    spawn_link = classmethod(spawn_link)

    all_actors = {}

    actor_id = property(lambda self: self._actor_id)

    def __init__(self, run=None):
        api.Greenlet.__init__(self, parent=api.get_hub().greenlet)

        if run is None:
            self._to_run = self.main
        else:
            self._to_run = lambda *args, **kw: run(self.receive, *args, **kw)

        self._actor_id = str(uuid.uuid1())
        self.all_actors[self.actor_id] = self

    #######
    ## Methods for general use
    #######

    def rename(self, name):
        """Change this actor's public name on this server.
        """
        del self.all_actors[self.actor_id]
        self._actor_id = name
        self.all_actors[name] = self

    def receive(self, *patterns):
        """Select a message out of this Actor's mailbox. If patterns
        are given, only select messages which match these shapes.
        Otherwise, select the next message.
        """
        if not patterns:
            if not self._mailbox:
                self._waiting = True
                api.get_hub().switch()
            return {object: object}, self._mailbox.pop(0)

        while True:
            for i, message in enumerate(self._mailbox):
                for pattern in patterns:
                    if shape.is_shaped(message, pattern):
                        del self._mailbox[i]
                        return pattern, message

            self._waiting = True
            api.get_hub().switch()

    def add_link(self, address, trap_exit=True):
        """Link the Actor at the given Address to this Actor.

        If this Actor has an unhandled exception, cast a message containing details
        about the exception to the Address. If trap_exit is True, also cast a message
        containing the Actor's return value when the Actor exits.
        """
        assert isinstance(address, Address)
        self._links.append(address)
        if trap_exit:
            self._exit_links.append(address)

    def main(self, *args, **kw):
        """If subclassing Actor, override this method to implement the Actor's
        main loop.
        """
        raise NotImplementedError("Implement in subclass.")

    def cooperate(self):
        self.sleep(0)

    def sleep(self, amount):
        api.sleep(amount)

    #######
    ## Implementation details
    #######

    def run(self):
        """Do not override.

        Run the Actor's main method in this greenlet. Send the function's
        result to the Actor's exit event. Also, catch exceptions and send
        messages with the exception details to any linked Actors, and send
        an exit message to any linked Actors after main has completed.
        """
        args, kw = self._args
        del self._args
        to_run = self._to_run
        del self._to_run
        try:
            result = to_run(*args, **kw)
            self._exit_event.send(result)
        except:
            if NOISY_ACTORS:
                print "Actor had an exception:"
                traceback.print_exc()
            result = None
            formatted = exc.format_exc()
            for link in self._links:
                link.cast({'address': self.address, 'exception': formatted})
            exc_info = sys.exc_info()
            self._exit_event.send_exception(*exc_info)
        for link in self._exit_links:
            link.cast({'address': self.address, 'exit': result})
        self.all_actors.pop(self.actor_id)

    def _cast(self, message):
        """For internal use.
        
        Address uses this to insert a message into this Actor's mailbox.
        """
        self._mailbox.append(simplejson.loads(message, object_hook=generate_address))
        if self._waiting:
            api.call_after_global(0, self.switch)


class Server(Actor):
    """An actor which responds to the call protocol by looking for the
    specified method and calling it.

    Also, Server provides start and stop methods which can be overridden
    to customize setup.
    """
    def start(self, *args, **kw):
        """Override to be notified when the server starts.
        """
        pass

    def stop(self, *args, **kw):
        """Override to be notified when the server stops.
        """
        pass

    def main(self, *args, **kw):
        """Implement the actor main loop by waiting forever for messages.
        
        Do not override.
        """
        self.start(*args, **kw)
        try:
            while True:
                pattern, message = self.receive(CALL_PATTERN)
                address = message['address']
                method = getattr(self, message['method'], None)
                if method is None:
                    address.cast({'response': message['call'], 'invalid_method': message['method']})
                    continue
                try:
                    result = method(message['message'])
                    address.cast({'response': message['call'], 'message': result})
                except Exception, e:
                    formatted = exc.format_exc()
                    address.cast({'response': message['call'], 'exception': formatted})
        finally:
            self.stop(*args, **kw)


class Gather(Actor):
    def main(self, spawnable_list):
        address_list = [spawn_link(x) for x in spawnable_list]

        messages = {}
        current_index = 0
        results = []
        for i in range(len(address_list)):
            _pattern, message = self.receive(
                {'exit': object, 'address': object},
                {'exception': object, 'address': object})

            messages[message['address']] = message
            if address_list[current_index] in messages:
                results.append(messages[address_list[current_index]])
                current_index += 1
        return results


def wait_all(*spawnable_list):
    if len(spawnable_list) == 1 and isinstance(spawnable_list[0], list):
        spawnable_list = spawnable_list[0]
    return spawn(Gather, spawnable_list).wait()

