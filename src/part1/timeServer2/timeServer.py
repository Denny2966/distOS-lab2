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

port = tcf.masterPort # master port, initialized as -1, since master is not identified yet
myport = tcf.self_port

allProcesses = []
initAllProcesses = []
elec_lock = threading.Lock()
masterFlag_lock = threading.Lock()
pid = None
myipAddress = None
isMaster = False
timeserver = None

class TimeServer(threading.Thread):
    offset = 0 
    def run(self):
        self.BerkleyTime()
    def BerkleyTime(self):
        """
        Implementation of the berkley algorthim,
        keeps track of other proceseses
        """
        global allProcesses
        print os.getpid(), "Master initializing."
        
        masterFlag_lock.acquire()
        isMasterSnapshot = isMaster
        masterFlag_lock.release()
        while isMasterSnapshot:
            time.sleep(5)
            rtts = []
            times = []
            for process in allProcesses:
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                    #calculate latency
                    print "calcualting rtt."
                    t0 = time.time()
                    t_tempt = proxy.getTime()
                    t1 = time.time()
                    print "getting time"
                    times.append(t_tempt - (t1-t0)/2.0 - t0)
                except Exception as e:
                    print e
                    print 'remove ', process, ' from master'
                    allProcesses.remove(process)
                    continue
                if len(times) > 0:
                    average = sum(times)/len(times)
                #else: # it is not possible
                #    average = os.times()[5]
            for process in allProcesses:
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                    index = allProcesses.index(process)
                    print "Setting offset for process", ()
                    proxy.setOffset(times[index] - average)
                except IndexError:
                    pass
                except Exception as e:
                    index = allProcesses.index(process)
                    allProcesses.remove(process)
                    del times[index]
            masterFlag_lock.acquire()
            isMasterSnapshot = isMaster
            masterFlag_lock.release()

class ServerRequestThread(threading.Thread):
    """
    Launches xml async server
    """
    def run(self):
        global port
        global myport
        global pid
        pid = tcf.process_id
        #for p in xrange(8100,8200):
        #myport = p
        try:
            print "starting server on", myport
            server = SimpleXMLRPCServer(('',myport)) # single thread server
            #server = AsyncXMLRPCServer(('', myport), SimpleXMLRPCRequestHandler)
            server.register_function(election, "election")
            server.register_function(amongstTheLiving, "amongstTheLiving")
            server.register_function(registerProcess, "registerProcess")
            server.register_function(setOffset, "setOffset")
            server.register_function(getTime, "getTime")
            server.register_function(getOffset, "getOffset")
            server.register_function(amIMaster, "amIMaster")
            server.register_function(remoteClaimIWon, "remoteClaimIWon")
            server.serve_forever()
        except Exception as e:
        #    print e
            raise e

class heartbeat(threading.Thread):
    def run(self):
        self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( port )) #proxy to master port
        global myport
        global allProcesses
        global myipAddress
        global pid
#        if tcf.masterIP == "127.0.0.1":
#            myipAddress = "127.0.0.1"
#        else:
#            myipAddress = socket.gethostbyname(socket.gethostname())
        myipAddress = tcf.self_IP
            
        # initialization
        cluster_info = tcf.cluster_info
        for i in cluster_info:
#            allProcesses = self.proxy.registerProcess(myipAddress, myport, pid)
            initAllProcesses = registerProcess(cluster_info[i][0], cluster_info[i][1], int(i))
        print 'wzd'
        print initAllProcesses

        election()
        while True:
            masterFlag_lock.acquire()
            isMasterSnapshot = isMaster
            masterFlag_lock.release()
            if isMasterSnapshot:
                diffAllProcesses = list(set(initAllProcesses)-set(allProcesses))
                for process in diffAllProcesses:
                    try:
                        print "Contacting process", process
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":"+ str( process[1] ))
                        proxy.amongstTheLiving()
                        print "success"
                        initAllProcesses = registerProcess(process[0], process[1], process[2])
                        election()
                        break
                    except:
                        continue
                time.sleep(10)
            else:
                try:
                    print "Contacting master..."
                    elec_lock.acquire()
                    self.proxy = xmlrpclib.ServerProxy("http://" + tcf.masterIP + ":"+ str( port )) #proxy to master port
                    elec_lock.release()
                    allProcesses = self.proxy.registerProcess(myipAddress, myport, pid)
                    print allProcesses
                    print "success"
                    time.sleep(10)
                except Exception as e:
                    elec_lock.release()
                    print e
                    election()

def amongstTheLiving(x):
    return True
            
def registerProcess(ipAddress,port,pid):
    """
    Makes the master process aware of the slave process
    Returns IP and port of other slaves.
    """
    global allProcesses
    if (ipAddress,port,pid) not in allProcesses:
        allProcesses.append((ipAddress,port,pid))
    return allProcesses

def election():
    """
    Bully election algorithm
    Elects new master if the current process dies
    The logic is correct, but there are maybe a lot of unecessary repetion of election requests; it is better to remove such repetion for achieving better performance
    """
    global allProcesses
    global isMaster
    global myport

    print "Starting election", os.getpid()
    print allProcesses
    winner = True

    for process in allProcesses:
        print process
        if pid >= process[2]:
            continue
        try:
            proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
        #    if (myport > process[1] or proxy.amIMaster()) and not isMaster: 
            print myport, process[1]
            result = proxy.election()
            print "Result:", result
            winner = False
            break

            #if result == "IWON":
                #elec_lock.acquire()
                #tcf.masterIP= process[0]
                #tcf.masterPort= process[1]
                #port= process[1]
                #elec_lock.release()
                #break
        except Exception as e:
            print e
            continue
    if winner:
        print "Won Election"
        global timeserver
        #elec_lock.acquire()
        masterFlag_lock.acquire()
        isMasterSnapshot = isMaster
        masterFlag_lock.release()
        if timeserver == None: # since there is perhaps more than one process that initiates a selection
            masterFlag_lock.acquire()
            isMaster = True
            masterFlag_lock.release()
            for process in allProcesses:
                print process
                if pid == process[2]:
                    continue
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
                    print 'hello++++++++++++++++++++++++++++++'
                    print process[0], ' ', process[1]
                    print 'hello++++++++++++++++++++++++++++++'
                    result = proxy.remoteClaimIWon(myipAddress, myport)
                    print result
                    print 'hello++++++++++++++++++++++++++++++'
                except Exception as e:
                    print e
                    continue
            timeserver = TimeServer()
            timeserver.daemon = True
            timeserver.start()
        elif not isMasterSnapshot: # master is lost but then achieve again 
            while timeserver.isAlive():
                time.sleep(2)
            masterFlag_lock.acquire()
            isMaster = True
            masterFlag_lock.release()
            for process in allProcesses:
                print process
                if pid == process[2]:
                    continue
                try:
                    proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
                    result = proxy.remoteClaimIWon(myipAddress, myport)
                except Exception as e:
                    print e
                    continue
            timeserver = TimeServer()
            timeserver.daemon = True
            timeserver.start()
        #elec_lock.release()
                
        return "IWON"
    masterFlag_lock.acquire()
    isMaster = False
    masterFlag_lock.release()
    print "Replying OK"
    return "OK"

def amIMaster():
    masterFlag_lock.acquire()
    isMasterSnapshot = isMaster
    masterFlag_lock.release()
    return isMasterSnapshot

def remoteClaimIWon(masterIP, masterPort):
    global port
    global isMaster

    elec_lock.acquire()
    tcf.masterIP = masterIP
    port = masterPort
    elec_lock.release()

    masterFlag_lock.acquire()
    print 'claim ', pid, ' not master'
    isMaster = False
    masterFlag_lock.release()
    return True

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
    try:
        s.start()
    except Exception as e:
        print 'wzd'
        print e
        sys.exit(1)
    time.sleep(5)
    h = heartbeat()
    h.daemon = True
    h.start()

if __name__ == '__main__':
    SetupServer()
    time.sleep(10)
    while True:
        time.sleep(2)
        masterFlag_lock.acquire()
        isMasterSnapshot = isMaster
        masterFlag_lock.release()
        if not isMasterSnapshot:
            print 'offset of ', pid, ' is ', getOffset()
        else:
            print pid
