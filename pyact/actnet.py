import eventlet
from eventlet.green import socket as _gsocket
from pyact import actor

# Active socket message interface. These are sent to the controlling
# actor:
#
#  {'actnet':'tcp', 'host':<host>, 'port':<port>, 'data':<data>}
#  {'actnet':'tcp_close', 'host':<host>, 'port':<port>}
#  {'actnet':'tcp_error', 'host':<host>, 'port':<port>,'error':<error>}
#  {'actnet':'udp', 'host':<host>,'port':<port>}
#
#

def connect(*args,**kw):
    active = kw.pop('active',False)
    s=eventlet.connect(*args,**kw)
    return _actsock(s,active)

def listen(*args,**kw):
    active = kw.pop('active',False)
    s=eventlet.listen(*args,**kw)
    return _actsock(s,active)

def socket(*args,**kw):
    active = kw.pop('active',False)
    s=_gsocket.socket(*args,**kw)
    return _actsock(s,active)


class InvalidActor(Exception): pass
class InvalidOption(Exception): pass

ONCE      = 'once'
FAMILIES  = [IPV4,IPV6] = ['ipv4','ipv6']
TYPES     = [TCP,UDP]   = ['tcp','udp']
TCP_ERROR,TCP_CLOSE     = 'tcp_error','tcp_close'

class _actsock(object):
    
    def __init__(self,sock,active):
        self._sock = sock
        self._actaddr = _curact()
        self._active = False
        self._family = _sockfam(sock)
        self._type   = _socktype(sock)
        self.setactive(active)
        self._gthread = None
        self._recvmax = 4096

    def __getattr__(self,attrib):
        return getattr(self._sock, attrib)

    def connect(self,address):
        r = self._sock.connect(address)
        self._activate()
        return r
    
    def connect_ex(self,address):
        r = self._sock.connect_ex(address)
        self._activate()
        return r

    def accept(self):
        connsock,addr = self._sock.accept()
        return _actsock(connsock,active=False),addr

    def setactor(self,address=None):
        if not address:
            address = _curact()
        self._actaddr = address
 
    def getactor(self):
        return self._actaddr

    def setactive(self,active):
        if not active:
            active = False
        else:
            if active != True and active != ONCE:
                raise InvalidOption("Invalid active value: "+str(active))
        if active == self._active:
            return # no change
        self._active = active
        self._activate()

    def getactive(self):
        return self._active

    def setrecvmax(self,recvmax):
        self._recvmax = recvmax

    def getrecvmax(self):
        return self._recvmax
        
    def _kill_poller(self):
        if self._gthread:
            self._gthread.kill()
            self._gthread=None

    def _activate(self):
        if self._active == False:
            self._kill_poll()
            return
        if self._type == UDP:
            self._gthread = eventlet.spawn(self._udp_poller)
            return
        try:
            peername = self._sock.getpeername()
        except _gsocket.error:
            # looks like not connected yet, should
            # retry on connect or connect_ex
            self._kill_poller()
            return
        self._gthread = eventlet.spawn(self._tcp_poller,peername)
        
    def _udp_poller(self):
        log("UDP poller started for",self._sock,self._actaddr)
        try:
            while True:
                (data,addr)=self._sock.recvfrom(self._recvmax)
                host,port = addr[:2]
                try:
                    actobj = self._actaddr._actor
                except (actor.DeadActor, actor.Killed):
                    log("UDP poller exiting since owner is dead")
                    break
                msg = {'actnet':UDP,'host':host,'port':port,'data':data}
                actobj._cast(msg,as_json=False)
                if self._active == ONCE:
                    self._active = False
                    break
        except eventlet.greenlet.GreenletExit:
            log("UDP poller killed")
            
    def _tcp_poller(self,peername):
        log("TCP poller started for",self._sock,self._actaddr)
        host,port = peername[:2]
        try:
            done = False
            while not done:
                data,error=None,None
                try:
                    data=self._sock.recv(self._recvmax)
                except _gsocket.error,e:
                    error=e.args
                try:
                    actobj = self._actaddr._actor
                except (actor.DeadActor, actor.Killed):
                    log("UDP poller exiting since owner is dead")
                    break
                if error is None:
                    if len(data)!=0:
                        msg = {'actnet':TCP,'host':host,'port':port,'data':data}
                    else:
                        msg = {'actnet':TCP_CLOSE,'host':host,'port':port}
                        done = True
                else:
                    msg = {'actnet':TCP_ERROR,'host':host,'port':port,'error':error}
                    done = True
                actobj._cast(msg,as_json=False)
                if self._active == ONCE:
                    self._active = False
                    break
        except eventlet.greenlet.GreenletExit:
            log("TCP poller killed")
            


def _sockfam(s):
    sockname_data = s.getsockname()
    # for IPv4 it is (ip,port)
    # for IPv6 it is (ip,port,flowinfo,scopeid)
    if len(sockname_data) == 2:
        return IPV4
    if len(sockname_data) == 4:
        return IPV6
    raise InvalidOption("Invalid socket family. Accpet only AF_INET and AF_INET6")

def _socktype(s):
    t=s.getsockopt(_gsocket.SOL_SOCKET, _gsocket.SO_TYPE)
    if t == _gsocket.SOCK_STREAM:
        return TCP
    if t == _gsocket.SOCK_DGRAM:
        return UDP
    raise InvalidOption("Invalid socket type. Accpet only SOCK_DGRAM and SOCK_STREAM")

def _curact():
    curact = eventlet.getcurrent()
    if hasattr(curact,'address'):
        return curact.address
    else:
        raise InvalidActor("Cannot determine current actor")
        


