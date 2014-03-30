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
port = 0

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
            time.sleep(10)
            rtts = []
            times = []
            for process in otherProcesses:
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                    #calculate latency
                    t = timeit.Timer('proxy.getTime()')
                    rtts.append(t.timeit()*2.0)
                    times.append( proxy.getTime())
                except:
                    otherProcesses.remove(process)
                    pass
                average = sum(times)/len(times)
                for process in otherProcesses:
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                        index = otherProcesses.index(process)
                        proxy.setOffset(times[index] - average)
                    except:
                        index = otherProcesses.index(process)
                        otherProcesses.remove(process)
                        del times[index]

class ServerRequestThread(threading.Thread):
    """
    Launches xml async server
    """
    def run(self):
        #heartbeat
        for p in xrange(8100,8200):
            port = p
            try:
                print "starting server", port
                server = AsyncXMLRPCServer(('', port), SimpleXMLRPCRequestHandler)
                server.register_function(election, "election")
                server.register_function(registerProcess, "registerProcess")
                server.register_function(setOffset, "setOffset")
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
        print port
        self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( tcf.masterPort )) #proxy to master port
        if tcf.masterIP == "127.0.0.1":
            ipAddress = "127.0.0.1"
        else:
            ipAddress = socket.gethostbyname(socket.gethostname())
        if not tcf.isMaster:
            try: 
                print "contacting master..."
                otherProcesses = self.proxy.registerProcess(ipAddress,port)
                while True:
                    time.sleep(1)
                    otherProcesses = self.proxy.registerProcess(ipAddress,port)
            except Exception as e:
                print e
                election()

def registerProcess(ipAddress,port):
    print os.getpid(), "registerProcess called"
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

def getOffset():
    return os.times[2] + TimeServer.offset


if __name__ == '__main__':
    timeserver = TimeServer()
    timeserver.start()
    s = ServerRequestThread()
    s.start()
    time.sleep(2)
    opt = ElectionManager()
    opt.start()
