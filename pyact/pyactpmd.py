"""
A port mapper for python actors. somewhat similar to 
Erlang's port mapper (EPMD).
"""


import eventlet
from eventlet.green import socket, subprocess
import sys, os


PORT = 5473


## ----------------------- External API ------------------------------------    
   
def connect_remote(host,port=PORT):
    """
    Connect to a remote port mapper from anywhere.
    """
    consock = _connect(host, port, tries=30, retrywait=1)
    if consock:
        return consock
    raise IOError("Could not connect to remote pyactpmd @ %s:%s"%(host,port))

def connect_local_andor_launch(port=PORT,logfile=None):
    """
    Connect to port mapper on the local machine. If the port
    mapper is not running, then start it, and then try connecting
    again.
    """
    consock = _connect('localhost', port)
    if consock:
        return consock
    _launch(port,logfile) # this spawns a subprocess that executes _this_ module
    eventlet.sleep(0.2)
    consock = _connect('localhost', port, tries=120, retrywait=0.2)
    if consock:
        return consock
    raise IOError("Could not launch and connect to pyactpmd on port %s"%port)



## ----------------------- Internal helper functions ------------------------

_registrations = {} # name : port

def _start(daemonize=True, logfile=None, port=PORT):
    """
    This function starts up the port mapper. This is run
    when 'python pyactmd.py' is executed. It will daemonize
    the process (both on win32 and posix systems), and 
    will start listening for connections.
    """
    if logfile:
        logfile = os.path.abspath(logfile)
    if daemonize:
        _daemonize(logfile, port)
    _redirect_io(logfile)
    try:
        server = _listen(('0.0.0.0',port))
    except socket.error,e:
        if e[0] == 98 or e[0] == 10048:
            print "port",PORT,"in use, another pyactpmd must be runing"
            return
        else:
            raise
    print "started pyactpmd on port: ",PORT,"pid:",os.getpid()
    sys.stdout.flush()
    while True:
        consock,address = server.accept()
        eventlet.spawn_n(_handlecon,consock,address)


def _dispatch(line):
    """
    Handle port mapper commands. On the first cut this is a
    simple line based protocol. It can handle these commands:
    
    'register name port'
    'lookup name'
    
    And the process itself can also handle the 'kill' command.
    That command is mostly for debugging as the port mapper
    is never explicitly killed or stopped after spawning.
    """
    splitl=line.split()
    if splitl[0] == 'register':
        if len(splitl)==2:
            name,port = splitl[1],None
        elif len(splitl)==3:
            name,port = splitl[1:]
            try:
                port = int(port)
            except:
                return 'error : invalid port'
        else:
            return 'error : invalid register args'
        _registrations[name] = port
        return 'ok'
    
    if splitl[0] == 'lookup':
        if len(splitl)!=2:
            return 'error : lookup args'
        return str(_registrations.get(splitl[1],''))
    
    return 'error : invalid command'

def _handlecon(consock,address):
    """
    Handle a single client connection.
    The connection can stays open and the 
    client can issue multiple commands
    in a sequential manner.
    """
    print "new connection from",address
    rfd = consock.makefile('r')
    wfd = consock.makefile('w')
    l = rfd.readline()
    clientport = None
    while l:
        l = l.strip()
        if l.lower() == 'kill':
            print "received kill command, exiting"
            wfd.write('ok\n')
            wfd.flush()
            sys.exit(0)
            break # shouldn't get here
        print "got line",l,"[",address,"]"
        try:
            r = _dispatch(l).strip()
        except Exception,e:
            r = 'error : %s' % e
        try:
            wfd.write(r+'\n')
            wfd.flush()
        except socket.error,e:
            if e[0] == 32:
                print "broken pipe from client",address
                break
            else:
                raise
        sys.stdout.flush()
        l = rfd.readline()
    rfd.close()
    wfd.close()
    consock.close()
    print "closed client connection",address
    sys.stdout.flush()

def _listen(addr):
    """
    Do not use eventlet.listen() as there is currently 
    a bug in it (see issue #83). That code always sets
    SO_REUSEADDR option on the socket, however on Windows
    that code does not work as expected. So handle the
    special case in a custom function.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if sys.platform != 'win32':
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen(1024)
    return sock

    
def _connect(host, port, tries=1, retrywait=1):
    try:
        return eventlet.connect((host,port))
    except socket.error:
        tries -= 1
        if tries <= 0:
            return None
        eventlet.sleep(retrywait)
        return _connect(port, tries-1, retrywait)

    
def _launch(port,logfile):
    """
    Run _this_ module in a separate process. That 
    process will daemonize the service and quickly
    exit. This function is supposed to be called
    from the external API. Users currently are
    not expected to manually or explicitely start
    the port mapper. The port mapper should be started
    the first time someone wants to use it.
    """
    args = [_get_python_executable(), __file__]
    if port:
        args += ['-p', str(port)]
    if logfile:
        args += ['-l', logfile]
    rcode = subprocess.call(args)
    return rcode



def _redirect_io(logfile):
    """
    The port mapper runs daemonized, so let it redirect
    its stout and stderr to a file for debugging.
    """
    if not logfile:
        return
    try:
        sys.stdout = open(logfile,'w',0)
    except (IOError,OSError):
        pass
    try:
        sys.stderr = open(logfile,'w',0)
    except (IOError,OSError):
        pass


def _daemonize(logfile,port):
    """
    Daemonization code. This handle the daemonization for
    both POSIX systems (that have os.fork) and Windows (which
    use a process creation API + 'detach' flag).

    NOTE(!): This function will return only on a POSIX based 
    system (and only when it is the last forked child). In 
    all other cases (POSIX but fork parent, or
    Windows), it will exit out of the process and never return.
    """
    # note: returns only on posix, when in child process
    if sys.platform != 'win32': # linux,unix,macosx
        _posix_daemonize()
        return # will return if last child process
    else: # win32
        _win32_daemonize(logfile,port)
        # never returns on win32, just create another process with -n option
        # then run os._exit(0)

def _posix_daemonize():
    sys.stdout.flush()
    sys.stderr.flush()
    if os.fork() > 0:
        os._exit(0)
    os.chdir('/')
    os.setsid()
    os.umask(0)
    if os.fork() > 0:
        os._exit(0)
    sys.stdout.flush()
    sys.stderr.flush()
    import resource
    mfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if mfd == resource.RLIM_INFINITY:
        mfd = 1024
    for fd in range(mfd):
        try:
            os.close(fd)
        except OSError:
            pass
    os.open('/dev/null', os.O_RDWR) # stdin = /dev/null
    os.dup2(0,1) # stdout = stdin = /dev/null
    os.dup2(0,2) # stderr = stdin = /dev/null


def _win32_daemonize(logfile,port):
    args = [_get_python_executable(), __file__, '-n']
    if logfile:
        args += ['-l', logfile]
    if port:
        args += ['-p', str(port)]
    DETACHED_PROCESS = 0x00000008
    subprocess.Popen(args, creationflags=DETACHED_PROCESS)
    os._exit(0)


def _get_python_executable():
    """
    This is used to get the current python executable in order 
    to spawn a new process and run _this_ module.
    """
    if sys.executable.lower().endswith('pythonservice.exe'):
        return os.path.join(sys.exec_prefix, 'python.exe')
    else:
        return sys.executable


if __name__=='__main__':
    from optparse import OptionParser
    p = OptionParser()
    p.add_option('-n','--nodetach',default=False,action='store_true',
                 help="Don't detach from terminal")
    p.add_option('-l','--logfile',default=None,
                 help="File to log stdout and stderr to.")
    p.add_option('-p','--port',default=PORT,type=int,
                 help="Port on which to listen for connections from actors.")
    opts,_ = p.parse_args()
    try:
        _start(not opts.nodetach, opts.logfile, opts.port)
    except (SystemExit, KeyboardInterrupt):
        print "Caught system interrupt, exiting"
        
    

