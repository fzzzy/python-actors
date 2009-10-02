
import traceback
import simplejson

from pyact import actor


def spawn_code(code_string):
    EvalActor.spawn(None, code_string)


class EvalActor(actor.Actor):
    def main(self, path, body):
        if path is not None:
            self.rename(path)
        try:
            exec body in {
                'actor_id': self.actor_id,
                'receive': self.receive,
                'cooperate': self.cooperate,
                'sleep': self.sleep,
                'spawn_code': spawn_code}
        except:
            traceback.print_exc()


class ActorApplication(object):
    def __call__(self, env, start_response):
        path = env['PATH_INFO'][1:]
        method = env['REQUEST_METHOD']
        if method == 'PUT':
            if not path:
                start_response('405 Method Not Allowed', [('Content-type', 'text/plain')])
                return 'Method Not Allowed\n'
            new_actor = EvalActor.spawn(path, env['wsgi.input'].read(int(env['CONTENT_LENGTH'])))
            start_response('202 Accepted', [('Content-type', 'text/plain')])
            return 'Accepted\n'
        elif method == 'POST':
            old_actor = actor.Actor.all_actors.get(path)
            if old_actor is None:
                start_response('404 Not Found', [('Content-type', 'text/plain')])
                return "Not Found\n"
            try:
                body = env['wsgi.input'].read(int(env['CONTENT_LENGTH']))
                msg = simplejson.loads(body)
            except Exception, e:
                traceback.print_exc()
                start_response('406 Not Acceptable', [('Content-type', 'text/plain')])
                return 'Not Acceptable\n'
            old_actor.address.cast(msg)
            start_response('202 Accepted', [('Content-type', 'text/plain')])
            return 'Accepted\n'            
        elif method == 'DELETE':
            old_actor = actor.Actor.all_actors.get(path)
            if old_actor is None:
                start_response('404 Not Found', [('Content-type', 'text/plain')])
                return "Not Found\n"
            old_actor.address.kill()
            start_response('200 OK', [('Content-type', 'text/plain')])
            return '\n'
        elif method == 'GET':
            if not path:
                start_response('200 OK', [('Content-type', 'text/plain')])
                return 'index\n'
            elif path == 'some-js-file.js':
                start_response('200 OK', [('Content-type', 'text/plain')])
                return 'some-js-file\n'            
            old_actor = actor.Actor.all_actors.get(path)
            if old_actor is None:
                start_response('404 Not Found', [('Content-type', 'text/plain')])
                return "Not Found\n"
            start_response('200 OK', [('Content-type', 'text/plain')])
            return str(old_actor)


app = ActorApplication()

