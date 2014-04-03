import time_config as tcf
import sys
import os
import threading
import xmlrpclib
import time
import subprocess
import socket
import timeit
import SocketServer

from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

otherProcesses = []
port = tcf.masterPort
myport = None
myipAddress = None
isMaster = False

class TimeServer(threading.Thread):
    offset = 0 
    def run(self):
        self.BerkleyTime()
    def BerkleyTime(self):
        """
        Implementation of the berkley algorthim,
        keeps track of other proceseses
        """
        global otherProcesses
        print os.getpid(), "Master initializing."
        while isMaster:
            time.sleep(5)
            rtts = []
            times = []
            for process in otherProcesses:
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                    #calculate latency
                    print "calcualting rtt."
                    t0 = time.time()
                    times.append(proxy.getTime())
                    t1 = time.time()
                    print "getting time"
                    rtts.append((t1-t0)*2.0)
                except Exception as e:
                    print e
                    otherProcesses.remove(process)
                    pass
                if len(times) > 0:
                    average = sum(times)/len(times)
                else:
                    average = os.times()[5]
                for process in otherProcesses:
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                        index = otherProcesses.index(process)
                        print "Setting offset for process", ()
                        proxy.setOffset(times[index] - average)
                    except IndexError:
                        pass
                    except Exception as e:
                        index = otherProcesses.index(process)
                        otherProcesses.remove(process)
                        del times[index]

class ServerRequestThread(threading.Thread):
    """
    Launches xml async server
    """
    def run(self):
        global port
        global myport
        for p in xrange(8100,8200):
            myport = p
            try:
                print "starting server on", myport
                server = AsyncXMLRPCServer(('', myport), SimpleXMLRPCRequestHandler)
                server.register_function(election, "election")
                server.register_function(amongstTheLiving, "amongstTheLiving")
                server.register_function(registerProcess, "registerProcess")
                server.register_function(setOffset, "setOffset")
                server.register_function(getTime, "getTime")
                server.register_function(getOffset, "getOffset")
                server.register_function(amIMaster, "amIMaster")
                server.serve_forever()
            except Exception as e:
                print e
                continue

class heartbeat(threading.Thread):
    def run(self):
        self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( port )) #proxy to master port
        global myport
        global otherProcesses
        global myipAddress
        if tcf.masterIP == "127.0.0.1":
            myipAddress = "127.0.0.1"
        else:
            myipAddress = socket.gethostbyname(socket.gethostname())
        otherProcesses = self.proxy.registerProcess(myipAddress, myport)
        election()
        try: 
            while True:
                if isMaster:
                    for process in otherProcesses:
                        print "Contacting process", process
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":"+ str( process[1] ))
                        proxy.amongstTheLiving()
                        print "success"
                        time.sleep(10)
                else:
                    print "Contacting master..."
                    otherProcesses = self.proxy.registerProcess(myipAddress, myport)
                    print otherProcesses
                    print "success"
                    time.sleep(10)
        except Exception as e:
            print e
            election()

def amongstTheLiving(x):
    return True
            
def registerProcess(ipAddress,port):
    """
    Makes the master process aware of the slave process
    Returns IP and port of other slaves.
    """
    global otherProcesses
    if (ipAddress,port) not in otherProcesses:
        otherProcesses.append((ipAddress,port))
    return otherProcesses

def election():
    """
    Bully election algorithm
    Elects new master if the current process dies
    """
    global otherProcesses
    global isMaster
    global myport
    print "Starting election", os.getpid()
    print otherProcesses
    winner = True
    for process in otherProcesses:
        print process
        try:
            proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
            if (myport > process[1] or proxy.amIMaster()) and not isMaster: 
                print myport, process[1]
                result = proxy.election()
                print "Result:", result
                winner = False
                if result == "IWON":
                    tcf.masterIP= process[0]
                    tcf.masterPort= process[1]
        except Exception as e:
            print e
            continue
    if winner:
        print "Won Election"
        isMaster = True
        timeserver = TimeServer()
        timeserver.start()
        return "IWON"
    isMaster = False
    print "Replying OK"
    return "OK"

def amIMaster():
    return isMaster

def amongstTheLiving():
    return True

def setOffset(offset):
    print os.getpid(), "offset set to:", offset
    TimeServer.offset = offset
    return True

def getOffset():
    return os.times()[4]+ TimeServer.offset

def getTime():
    return os.times()[4]

def SetupServer():
    s = ServerRequestThread()
    s.daemon = True
    s.start()
    time.sleep(5)
    h = heartbeat()
    h.daemon = True
    h.start()

if __name__ == '__main__':
    SetupServer()
    time.sleep(10)
    if not isMaster:
        print getOffset()
