import unittest
import eventlet
from eventlet.green import socket
from pyact import actor, actnet

def echo_tcp_line():
    """
    spawn a tcp server, listen, on a port,
    accept one client only, read line data,
    echo data back to client, until connection
    is closed or string 'die' is received.
    
    return port it is listening on
    """
    server = eventlet.listen(('0.0.0.0',0))
    port = server.getsockname()[1]
    def serve():
        sclient,_=server.accept()
        sfd = sclient.makefile('rw')
        while True:
            data = sfd.readline().strip()
            if not data:
               break
            if data=='die':
                sclient.close()
                break
            sfd.write(data+'\n')
            sfd.flush()
    eventlet.spawn_n(serve)
    eventlet.sleep(0)
    return port

def echo_tcp():
    """
    spawn a tcp server, listen, on a port,
    accept one client only, read data and echo it
    back to the sender, until connection
    is closed
    
    return port it is listening on
    """
    server = eventlet.listen(('0.0.0.0',0))
    port = server.getsockname()[1]
    def serve():
        sclient,_=server.accept()
        while True:
            data = sclient.recv(2048)
            if not data:
                break
            sclient.sendall(data)
    eventlet.spawn_n(serve)
    eventlet.sleep(0)
    return port


def echo_udp():
    """
    spawn a udp server, bind to a local port,
    receive data, echo data back to client,
      
    return port the socket is receiving on
    """
    usock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    usock.bind(('0.0.0.0',0))
    port = usock.getsockname()[1]
    def serve():
        while True:
            data,addr=usock.recvfrom(2048)
            if not data:
                break
            usock.sendto(data,addr)
    eventlet.spawn_n(serve)
    eventlet.sleep(0)
    return port

class TestActnet(unittest.TestCase):

    def test_connect_tcp(self):
        class EchoClient(actor.Actor):
            def main(self,port):
                s=actnet.connect(('localhost',port),active=True)
                fd = s.makefile('rw')
                fd.write('echo1\n')
                fd.flush()
                return self.receive({'actnet':'tcp','data':'echo1\n'},timeout=0.1)
        port = echo_tcp_line()
        p,m = EchoClient.spawn(port).wait()
        assert p is not None,"Timeout"
        assert 'data' in m
        assert m['data']=='echo1\n'


    def test_connect_tcp_dialog(self):
        class EchoMultiple(actor.Actor):
            def main(self,port):
                s=actnet.connect(('localhost',port),active=True)
                fd = s.makefile('rw')
                for i in range(250):
                    fd.write(str(i)+'\n')
                    fd.flush()
                    p,m=self.receive({'actnet':'tcp','data':str(i)+'\n'},timeout=0.1)
                    if p is None:
                        return False
                return True
        port = echo_tcp_line()
        assert EchoMultiple.spawn(port).wait()


    def test_connect_tcp_large_data(self):
        class EchoStream(actor.Actor):
            def main(self,port,recvmax):
                s=actnet.connect(('localhost',port),active=True)
                N=100000
                data='x'*N
                s.sendall(data)
                rdata=''
                s.setrecvmax(recvmax)
                while True:
                    p,m=self.receive({'actnet':'tcp'},timeout=0.1)
                    if p is None:
                        break
                    rchunk = m['data']
                    rdata+=rchunk
                if len(rdata)!=len(data):
                    return False
                else:
                    return True
        port = echo_tcp()
        assert EchoStream.spawn(port,4096).wait()
        port = echo_tcp()
        assert EchoStream.spawn(port,1024).wait()


    def test_tcp_close(self):
        class EchoClient(actor.Actor):
            def main(self,port):
                s=actnet.connect(('localhost',port),active=True)
                s.sendall('die\n')
                eventlet.sleep(0.1)
                #s.close()
                return self.receive({'actnet':'tcp_close'},timeout=0.1)
        port = echo_tcp_line()
        p,m = EchoClient.spawn(port).wait()
        assert p

    def test_udp_echo(self):
        class UDPEcho(actor.Actor):
            def main(self,port):
                s=actnet.socket(socket.AF_INET,socket.SOCK_DGRAM,active=True)
                s.sendto('hi',('localhost',port))
                return self.receive({'actnet':'udp','data':'hi'},timeout=0.1)
        port = echo_udp()
        p,m = UDPEcho.spawn(port).wait()
        assert p
   
    def test_udp_dialog(self):
        class UDPDialog(actor.Actor):
            def main(self,port):
                s=actnet.socket(socket.AF_INET,socket.SOCK_DGRAM,active=True)
                s.bind(('0.0.0.0',0))
                for i in range(250):
                    s.sendto(str(i),('localhost',port))
                    p,m=self.receive({'actnet':'udp','data':str(i)},timeout=0.1)
                    if p is None:
                        return False
                return True
        port = echo_udp()
        assert UDPDialog.spawn(port).wait()


if __name__ == '__main__':
    unittest.main()

