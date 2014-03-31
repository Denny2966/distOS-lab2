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

class TimeServer(threading.Thread):
    def run(self):
        self.BerkleyTime()
    def BerkleyTime(self):
        """
        Implementation of the berkley algorthim,
        keeps track of other proceseses
        """
        global otherProcesses
        if tcf.isMaster:
            print os.getpid(), "Master initializing."
        while tcf.isMaster:
            time.sleep(5)
            rtts = []
            times = []
            print otherProcesses
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
        #heartbeat
        for p in xrange(8100,8200):
            port = p
            try:
                print "starting server", port
                server = AsyncXMLRPCServer(('', port), SimpleXMLRPCRequestHandler)
                server.register_function(election, "election")
                server.register_function(registerProcess, "registerProcess")
                server.register_function(setOffset, "setOffset")
                server.register_function(getTime, "getTime")
                server.register_function(getOffset, "getOffset")
                server.serve_forever()
            except Exception as e:
                print e
                continue

class ElectionManager(threading.Thread):
    """
    Runs heartbeat to check if master is still active, gets list of processes
    Runs election if failure is detected
    """
    def run(self):
        self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( tcf.masterPort )) #proxy to master port
        if tcf.masterIP == "127.0.0.1":
            ipAddress = "127.0.0.1"
        else:
            ipAddress = socket.gethostbyname(socket.gethostname())
        if not tcf.isMaster:
            try: 
                print "contacting master..."
                otherProcesses = self.proxy.registerProcess(ipAddress,port)
                "success."
                while True:
                    time.sleep(2)
                    otherProcesses = self.proxy.registerProcess(ipAddress,port)
            except Exception as e:
                print e
                election()

def registerProcess(ipAddress,port):
    if (ipAddress,port) not in otherProcesses:
        print "Registering Process", (ipAddress,port)
        otherProcesses.append((ipAddress,port))
    return otherProcesses

def election():
    print "Starting election", os.getpid()
    winner = True
    for process in otherProcesses:
        try:
            if port > process[1]: 
                proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + process[1])
                result = proxy.election()
                winner = False
                if result == "IWON":
                    tcf.masterIP = process[0]
                    tcf.masterPort  = process[1]
        except:
            continue
    if winner:
        print "process", os.getpid(), "Won Election"
        tcf.isMaster = True
        timeserver = TimeServer()
        timeserver.start()
        return "IWON"
    return "OK"

def setOffset(offset):
    print os.getpid(), "offset set to:", offset
    TimeServer.offset = offset
    return True

def getOffset():
    return os.times[5] + TimeServer.offset

def getTime():
    return os.times()[4]


def SetupServer():
    timeserver = TimeServer()
    timeserver.start()
    s = ServerRequestThread()
    s.start()
    time.sleep(2)
    opt = ElectionManager()
    opt.start()

if __name__ == '__main__':
    SetupServer()
