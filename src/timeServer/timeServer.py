import time_config as tcf
import sys
import os
import threading
import xmlrpclib
import time
import subprocess
import socket
import timeit
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

#whoever has the lowest port number for is the leader.
class TimeServer(threading.Thread):
    otherProcesses = []
    port = 0
    offset = 0
    def run(self):
        self.BerkleyTime()
    def BerkleyTime(self):
        """
        Implementation of the berkley algorthim,
        keeps track of other proceseses
        """
        if tcf.isMaster:
            print os.getpid(), "Master initializing."
        while tcf.isMaster:
            time.sleep(10)
            rtts = []
            times = []
            for process in TimeServer.otherProcesses:
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                    #calculate latency
                    t = timeit.Timer('proxy.getTime()')
                    rtts.append(t.timeit()*2.0)
                    times.append( proxy.getTime())
                except:
                    TimeServer.otherProcesses.remove(process)
                    pass
                average = sum(times)/len(times)
                for process in TimeServer.otherProcesses:
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                        index = TimeServer.otherProcesses.index(process)
                        proxy.setOffset(times[index] - average)
                    except:
                        index = TimeServer.otherProcesses.index(process)
                        TimeServer.otherProcesses.remove(process)
                        del times[index]

class OtherProcess(threading.Thread):
    """
    Launches XML async server
    Runs heartbeat to check if master is still active, gets list of processes
    Runs election if failure is detected
    """
    def run(self):
        self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( tcf.masterPort ))
        #heartbeat
        for port in xrange(8100,8200):
            TimeServer.port = port
            try:
                server = AsyncXMLRPCServer(('', port), SimpleXMLRPCRequestHandler)
                server.register_function(election, "election")
                server.register_function(registerProcess, "registerProcess")
                server.register_function(setOffset, "setOffset")
                server.serve_forever()
            except:
                continue
        if tcf.masterIP == "127.0.0.1":
            ipAddress = "127.0.0.1"
        else:
            ipAddress = socket.gethostbyname(socket.gethostname())
        if not tcf.isMaster:
            TimeServer.otherProcesses = self.proxy.registerProcess(ipAddress,TimeServer.port)
        while True:
            time.sleep(1)
            try:
                TimeServer.otherProcesses = self.proxy.registerProcess(ipAddress,self.port)
            except:
                election()

def registerProcess(ipAddress,port):
    if (ipAddress,port) not in otherProcesses:
        print "Registering Process", (ipAddress,port)
        otherProcesses.append((ipAddress,port))
    return TimeServer.otherProcesses

def election():
    print "Starting election", os.getpid()
    winner = True
    for process in TimeServer.otherProcesses:
        try:
            if TimeServer.port > process[1]: 
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
        timeserver.run()
        return "IWON"
    return "OK"

def setOffset(offset):
    print os.getpid(), "offset set to:", offset
    TimeServer.offset = offset

def getOffset():
    return os.times[2] + TimeServer.offset


if __name__ == '__main__':
    timeserver = TimeServer()
    timeserver.run()
    opt = OtherProcess()
    opt.run()
