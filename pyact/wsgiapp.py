
import traceback
import eventlet
from pyact import actor, shape


def spawn_code(code_string):
    EvalActor.spawn(None, code_string)


class EvalActor(actor.Actor):
    def main(self, path, body):
        if path is not None:
            self.rename(path)
        try:
            exec body in {
                'actor_id': self.actor_id,
                'address': self.address,
                'receive': self.receive,
                'cooperate': self.cooperate,
                'sleep': self.sleep,
                'lookup': actor.RemoteAddress.lookup,
                'spawn_code': spawn_code,
                'spawn_remote': actor.spawn_remote}, vars(self)
        except:
            traceback.print_exc()


RSP_PAT = {'response':str, 'message':object}
EXC_PAT = {'response':str, 'exception':object}
INV_PAT = {'response':str, 'invalid_method':str}

class LocalCaller(actor.Actor):
    """
    Performs a local call on behalf of a remote caller and 
    immediately exists
    """
    def main(self,local_addr,message_id,method,message,timeout):
        my_address = eventlet.getcurrent().address
        local_addr | {'call':message_id,
                      'method':method,
                      'address':my_address,
                      'message':message}
        if timeout is None:
            cancel = None
        else:
            cancel = eventlet.Timeout(timeout,eventlet.TimeoutError)
        RSP = {'response':message_id,'message':object}
        EXC = {'response':message_id,'exception':object}
        INV = {'response':message_id,'invalid_method':str}
        _,res = self.receive(RSP,EXC,INV)
        return res

class ActorApplication(object):

    def __call__(self, env, start_response):
        path = env['PATH_INFO'][1:]
        method = 'do_'+env['REQUEST_METHOD']
        if hasattr(self,method):
            return getattr(self,method)(path,env,start_response)

    def do_PUT(self,path,env,start_response):
        if not path:
            start_response('405 Method Not Allowed', [('Content-type', 'text/plain')])
            return 'Method Not Allowed\n'
        new_actor = EvalActor.spawn(path, env['wsgi.input'].read(int(env['CONTENT_LENGTH'])))
        start_response('202 Accepted', [('Content-type', 'text/plain')])
        return 'Accepted\n'

    def do_POST(self,path,env,start_response):
        local_address = 'http://%s/' % (env['HTTP_HOST'], )
        old_actor = actor.Actor.all_actors.get(path)

        if old_actor is None:
            start_response('404 Not Found', [('Content-type', 'text/plain')])
            return "Not Found\n"

        try:
            body = env['wsgi.input'].read(int(env['CONTENT_LENGTH']))
            def generate_custom(obj):
                if obj.keys() == ['address']:
                    address = obj['address']
                    if address.startswith(local_address):
                        obj = {'address': address[len(local_address):]}
                return actor.generate_custom(obj)
            msg = actor.json.loads(body, object_hook=generate_custom)
        except Exception, e:
            traceback.print_exc()
            start_response('406 Not Acceptable', [('Content-type', 'text/plain')])
            return 'Not Acceptable\n'

        if not shape.is_shaped(msg,actor.REMOTE_CALL_PATTERN):
            old_actor.address.cast(msg)
            start_response('202 Accepted', [('Content-type', 'text/plain')])
            return 'Accepted\n'
        
        # handle a remote call
        try:
            rmsg = LocalCaller.spawn(local_addr=old_actor.address,
                                     message_id=msg['remotecall'],
                                     method=msg['method'],
                                     message=msg['message'],
                                     timeout=msg['timeout']).wait()
        except eventlet.TimeoutError:
            start_response('408 Request Timeout',[('Content-type','text/plain')])
            return actor.json.dumps({'timeout':msg['timeout']})+'\n'

        resp_str = actor.json.dumps(rmsg, default=_remote_handle_custom)+'\n'
        if shape.is_shaped(rmsg, RSP_PAT):
            start_response('202 Accepted',[('Content-type','application/json')])
        elif shape.is_shaped(rmsg, INV_PAT):
            start_response('404 Not Found',[('Content-type','application/json')])
        else:
            start_response('406 Not Acceptable',[('Content-type','application/json')])
        return resp_str


    def do_DELETE(self,path,env,start_response):
        old_actor = actor.Actor.all_actors.get(path)
        if old_actor is None:
            start_response('404 Not Found', [('Content-type', 'text/plain')])
            return "Not Found\n"
        old_actor.address.kill()
        start_response('200 OK', [('Content-type', 'text/plain')])
        return '\n'

    def do_HEAD(self,path,env,start_response):
        old_actor = actor.Actor.all_actors.get(path)
        if old_actor is None:
            start_response('404 Not Found', [('Content-type', 'text/plain')])
            return "\n"
        start_response('200 OK', [('Content-type', 'application/json')])
        return "\n"

    def do_GET(self,path,env,start_response):
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
        start_response('200 OK', [('Content-type', 'application/json')])
        local_address = 'http://%s/' % (env['HTTP_HOST'], )

        to_dump = dict([(x, y) for (x, y) in vars(old_actor).items() if not x.startswith('_')])
        return actor.json.dumps(to_dump, default=_remote_handle_custom) + '\n'
        

def _remote_handle_custom(obj):
    """Custom json handling of remote addresses.
    Falls back on actor.handle_custom to handle
    Binary or other objects.
    """
    if isinstance(obj, actor.Address):
        return {'address': local_address + obj.actor_id}
    return actor.handle_custom(obj)
    

app = ActorApplication()

