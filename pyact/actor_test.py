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

import unittest

from eventlet import api
from eventlet import coros

from pyact import actor


EXCEPTION_MARKER = "Child had an exception"


def foo(receive):
    return 2 + 2


class Supervisor(actor.Actor):
    child_type = property(lambda self: foo)

    def do_receive(self, address):
        return self.receive(
                    {'exit': object, 'address': address},
                    {'exception': object, 'address': address})
    def main(self):
        address = actor.spawn_link(self.child_type)
        pattern, message = self.do_receive(address)
        if 'exception' in pattern:
            return EXCEPTION_MARKER
        return message['exit']


def exception(receive):
    raise RuntimeError(EXCEPTION_MARKER)


class TestActor(unittest.TestCase):
    def test_basic_actor(self):
        self.assertRaises(NotImplementedError, actor.spawn(actor.Actor).wait)

    def test_wait(self):
        """Call spawn with a function that returns 2 + 2.
        Assert that wait returns the value the function returns.
        """
        result = actor.spawn(foo).wait()
        self.assertEquals(result, 4)

    def test_linked_wait(self):
        """Spawn Supervisor, which calls spawn_link with
        a function that returns 2 + 2 and returns the result extracted
        out of the link exit.
        """
        result = actor.spawn(Supervisor).wait()
        self.assertEquals(result, 4)

    def test_wait_exception(self):
        """Call spawn with a function that raises an exception, and assert
        that calling wait raises the same exception.
        """
        self.assertRaises(RuntimeError, actor.spawn(exception).wait)

    def test_linked_exception(self):
        """Spawn an Actor which calls spawn_link with a function that
        raises an exception. The ExceptionSupervisor will get a link message
        when the exception occurs, at which point it returns the
        EXCEPTION_MARKER.
        """
        class ExceptionSupervisor(Supervisor):
            child_type = property(lambda self: exception)

        result = actor.spawn(ExceptionSupervisor).wait()
        self.assertEquals(result, EXCEPTION_MARKER)

    def test_actor_linked_to_actor(self):
        """Start an Actor which calls spawn_link on another actor. When
        ChildSupervisor gets the link exit message, it returns the result.
        Assert that calling wait on ChildSupervisor results in the return
        result of Child.
        """
        class Child(actor.Actor):
            def main(self):
                return "Hi There"
        
        
        class ChildSupervisor(Supervisor):
            child_type = Child

        result = actor.spawn(ChildSupervisor).wait()
        self.assertEquals(result, "Hi There")

    def test_unconditional_receive(self):
        """Assert that calling receive with no arguments properly selects
        messages from the Actor's mailbox.
        """
        class UnconditionalSupervisor(Supervisor):
            def do_receive(self, address):
                return self.receive()

        result = actor.spawn(UnconditionalSupervisor).wait()

    def test_call(self):
        """Start an Actor that starts another Actor and then uses
        call on the Address. Assert that the parent gets a response
        from the child and returns it.
        """
        class CallChild(actor.Actor):
            def main(self):
                pattern, message = self.receive(
                    {'call': str, 'address': object, 'method': str, 'message': object})
                message['address'].cast(
                    {'response': message['call'], 'message': 'Hi There'})

        class CallParent(actor.Actor):
            def main(self):
                return actor.spawn(CallChild).call('method', {})

        self.assertEquals(actor.spawn(CallParent).wait(), "Hi There")

        class TimeoutCallParent(actor.Actor):
            def main(self):
                return actor.spawn(CallChild).call('method', {}, 1)

        self.assertEquals(actor.spawn(TimeoutCallParent).wait(), "Hi There")

    def test_timeout(self):
        """Start an Actor that starts another Actor that accepts a call and
        never responds. The parent calls the child with a small timeout value.
        Assert that waiting for the parent raises a TimeoutError.
        """
        class TimeoutChild(actor.Actor):
            def main(self):
                pattern, message = self.receive(
                    {'call': str, 'address': object, 'message': object})
                # Don't respond
        
        class TimeoutParent(actor.Actor):
            def main(self):
                return actor.spawn(TimeoutChild).call('method', {}, timeout=0.1)

        self.assertRaises(api.TimeoutError, actor.spawn(TimeoutParent).wait)

    def test_dead_actor(self):
        class DeadTest(actor.Actor):
            def main(self):
                child = actor.spawn(foo)
                child.wait()
                child.cast({'hello': 'there'})

        self.assertRaises(actor.DeadActor, actor.spawn(DeadTest).wait)

    def test_manual_link(self):
        class LinkTest(actor.Actor):
            def main(self):
                child = actor.spawn(foo)
                child.link()
                cancel = api.exc_after(0.1, api.TimeoutError)
                result = self.receive(
                    {'exit': object, 'address': object},
                    {'exception': object, 'address': object})
        actor.spawn(LinkTest).wait()

    def test_kill(self):
        def forever(receive):
            api.sleep(5000)

        class KillTest(actor.Actor):
            def main(self):
                address = actor.spawn(forever)
                try:
                    address.call('method', {}, 0.1)
                except api.TimeoutError:
                    pass

                address.kill()
                return address.wait()

        self.assertRaises(actor.Killed, actor.spawn(KillTest).wait)

    def test_wait_all(self):
        class WaitAll(actor.Actor):
            def main(self):
                def foo(receive):
                    return 1
                def bar(receive):
                    return 2
                def baz(receive):
                    return 3
                result1 = list(actor.wait_all(foo, bar, baz))
                result2 = list(actor.wait_all([foo, bar, baz]))
                return result1, result2

        cancel = api.exc_after(1, api.TimeoutError)
        result1, result2 = actor.spawn(WaitAll).wait()
        cancel.cancel()

        result1 = [x.get('exit') for x in result1]
        result2 = [x.get('exit') for x in result2]
        self.assertEquals([1,2,3], result1)
        self.assertEquals([1,2,3], result2)


THE_RESULT = "This is the result"


class TestServer(unittest.TestCase):
	def test_server(self):
		class SimpleServer(actor.Server):
			def foo(self, message):
				return THE_RESULT

		class SimpleClient(actor.Actor):
			def main(self):
				server = SimpleServer.spawn()
				return server.call('foo', None)

		cancel = api.exc_after(1, api.TimeoutError)
		result = SimpleClient.spawn().wait()

		self.assertEquals(result, THE_RESULT)

	def test_exception(self):
		class SimpleServer(actor.Server):
			def foo(self, message):
				raise RuntimeError("Exception!")

		class SimpleClient(actor.Actor):
			def main(self):
				server = SimpleServer.spawn()
				return server.call('foo', None)

		self.assertRaises(actor.RemoteException, SimpleClient.spawn().wait)

	def test_bad_method_name(self):
		class SimpleServer(actor.Server):
			def foo(self, message):
				return THE_RESULT

		class SimpleClient(actor.Actor):
			def main(self):
				server = SimpleServer.spawn()
				return server.call('bar', None)

		self.assertRaises(actor.RemoteAttributeError, SimpleClient.spawn().wait)

	def test_start_stop(self):
		mutate_me = {}
		class SimpleServer(actor.Server):
			def start(self):
				mutate_me['start'] = True

			def foo(self, message):
				mutate_me['foo'] = True

			def stop(self):
				mutate_me['stop'] = True

		class SimpleClient(actor.Actor):
			def main(self):
				server = SimpleServer.spawn()
				server.call('foo', None)
				server.kill()

		SimpleClient.spawn().wait()

		self.assertEqual(mutate_me.get('start'), True)
		self.assertEqual(mutate_me.get('foo'), True)
		self.assertEqual(mutate_me.get('stop'), True)


if __name__ == '__main__':
    unittest.main()

