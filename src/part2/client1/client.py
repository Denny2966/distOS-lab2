#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

import client_config as cf
import threading
import SocketServer
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
#import SimpleXMLRPCServer
import xmlrpclib
import socket
import re
import numpy as np
import random
import socket
import time

import heapq as hp
MAX_HEAP_SIZE = 1000
heap = []
c_time = 0
heap_size = 0
ack_num_dict = {}
process_num = 0
heap_lock = threading.Lock()
dict_lock = threading.Lock()

pid = cf.process_id
cluster_info = cf.cluster_info

myipAddress = cluster_info[str(pid)][0]
myport = cluster_info[str(pid)][1]

all_processes = []

# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
score_board = [[0 for x in xrange(4)] for x in xrange(3)]

team_name_list = ['Gauls', 'Romans']
medal_type_list = ['Gold', 'Silver', 'Bronze']
event_type_list = ['Curling', 'Skating', 'Skiing']

team_name_dict = {"Gauls":0, "Romans":1}
medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}

t_file = None
s_file = None

t_file_name = './log/tally_board.out'
s_file_name = './log/score_board.out'

#global sb_lock
#global output_lock
#global s_file_lock

def get_team_name_index(teamName):
	team_name_index = -1
	if team_name_dict.has_key(teamName): 	
		team_name_index = team_name_dict[teamName]
	return team_name_index

def get_medal_type_index(medalType):
	medal_type_index = -1
	if medal_type_dict.has_key(medalType): 	
		medal_type_index = medal_type_dict[medalType]
	return medal_type_index

def get_event_type_index(eventType):
	event_type_index = -1
	if event_type_dict.has_key(eventType): 	
		event_type_index = event_type_dict[eventType]
	return event_type_index

def get_rand_value(value_list):
    return value_list[random.randrange(0, len(value_list))]

class ClientObject:
        def __init__(self):
                pass

	def get_medal_tally(self, s, team_name = 'Gauls'):
		result = s.get_medal_tally(team_name)

		team_name_index = get_team_name_index(team_name)
		if team_name_index != -1 :
			tally_board[team_name_index] = result

			# write obtained medal tally into the output file
			with open(t_file_name, 'r+') as t_file :
				t_file_data = t_file.readlines()
				t_file_data[team_name_index] = str(team_name) + ': ' + str(result) + '\n'
				t_file.seek(0)
				t_file.writelines(t_file_data)
		return result

	def get_score(self, s, event_type = 'Curling'):
		result = s.get_score(event_type)

		event_type_index = get_event_type_index(event_type)
		if event_type_index != -1:
			score_board[event_type_index] = result

			# write obtained scores into the output file
			with open(s_file_name, 'r+') as s_file :
				s_file_data = s_file.readlines()
				s_file_data[event_type_index] = str(event_type) + ': ' + str(result)  + '\n'
				s_file.seek(0)
				s_file.writelines(s_file_data)
		return result

	def start(self, poisson_lambda, simu_len, get_score_pb):
            global heap_lock
            t = np.random.rand(poisson_lambda*simu_len) * simu_len
            t.sort()
            t[1:len(t)-1] = t[1:len(t)-1] - t[0:len(t)-2]

            global pid
            global c_time
            global s_list

            try:
                for t_val in t:
                    time.sleep(t_val)

                    heap_lock.acquire()
                    c_time_snapshot = c_time
                    heap_lock.release()

                    if np.random.rand(1) < get_score_pb:
                            req_type = 'score'
                            req_para = get_rand_value(event_type_list)
                    else:
                            req_type = 'medal'
                            req_para = get_rand_value(team_name_list)
  
                    for s in s_list:
                            try:
                                    s.record_request((req_type, req_para,), (c_time_snapshot+1, pid,))
                            except Exception as e:
                                    print e
                                    time.sleep(0.1)
                                    try:
                                        s.record_request((req_type, req_para,), (c_time_snapshot+1, pid,))
                                    except:
                                        pass


            except socket.error, (value,message):
                print "Could not open socket to the server: " + message
                return
            except :
                info = sys.exc_info()
                print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
                return

def record_request(request, l_time):
    global s_list
    global heap_lock
    global MAX_HEAP_SIZE
    global heap_size
    global heap
    global c_time

    ele = tuple(l_time)
    flag = True
    heap_lock.acquire()
    if MAX_HEAP_SIZE <= heap_size:
        flag = False
    else:
        hp.heappush(heap, ele)
        c_time += 1
        heap_size += 1
#        for s in s_list:
#            try:
#                s.send_ack(l_time, request)
#            except Exception as e:
#                print e
#                pass

    heap_lock.release()
    return flag

def send_ack(l_time, pid):
    global ack_num_dict
    global dict_lock

    dict_lock.acquire()
    l_time = tuple(l_time)
    if l_time in ack_num_dict:
        ack_num_dict[l_time] += 1
    else:
        ack_num_dict[l_time] = 1
    dict_lock.release()

    return True

def check_alive():
    return True

class ServerThread(threading.Thread):
    """a RPC server listening to push request from the server of the whole system"""
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port

        self.localServer = AsyncXMLRPCServer(('', port), SimpleXMLRPCRequestHandler) #SimpleXMLRPCServer(('', port))

        self.localServer.register_function(record_request, 'record_request')
        self.localServer.register_function(check_alive, 'check_alive')
        self.localServer.register_function(send_ack, 'send_ack')
    def run(self):
        self.localServer.serve_forever()

class HeapThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        global heap_lock
        global client_object
        global heap_size
        global heap
        global ack_num_dict
        global process_num
        global s_list
        global pid

        have_sent_ack = set()
        ExpireCount = 3
        flag_count = 0
        ele_pre = None

        remove_list = []

        while True:
            heap_lock.acquire()
            if heap_size == 0:
                pass
            else:
#                print 'remove list', remove_list
                time.sleep(0.1)
#                print 'sent ack list: ', list(have_sent_ack)
                for l_time in heap:
                    if l_time not in have_sent_ack:
                        have_sent_ack.add(l_time)
                        for s in s_list:
                            try:
                                s.send_ack(l_time, pid)
                            except Exception as e:
                                print e
                                time.sleep(0.1)
                                try:
                                    s.send_ack(l_time, pid)
                                except:
                                    pass
                dict_lock.acquire()
                try:
                    if ele_pre == heap[0]:
                        flag_count += 1
                    else:
                        flag_count = 0

                    if flag_count >= ExpireCount:
                        if heap[0] in ack_num_dict:
                            del ack_num_dict[heap[0]]
                        ele = hp.heappop(heap)
                        print '****', ele
                        heap_size -= 1
                        flag_count = 0

                    if heap_size > 0:
                        ele_pre = heap[0]
                    else:
                        ele_pre = None
                    while heap_size > 0 and heap[0] in ack_num_dict and ack_num_dict[heap[0]] >= process_num:
                        del ack_num_dict[heap[0]]
                        tmp = hp.heappop(heap)
                        remove_list += [tmp]
                        heap_size -= 1
                        flag_count = 0
                except Exception as e:
                    print e
                dict_lock.release()
            heap_lock.release()
            time.sleep(1+np.random.rand()*2)

if __name__ == "__main__":
        for i in cluster_info:
                all_processes.append((cluster_info[i][0], cluster_info[i][1], int(i)))
        process_num = len(all_processes)

        server = ServerThread(myport)
        server.daemon = True; # allow the thread exit right after the main thread exits by keyboard interruption.
        server.start() # The server is now running

        s_list = []
        for process in all_processes:
                URL = "http://" + process[0] + ":"+ str( process[1] )
                print URL
                s_list.append(xmlrpclib.ServerProxy(URL))

        while True:
                try:
                        for i in range(len(all_processes)):
                                print i
                                s_list[i].check_alive()
                        break
                except Exception as e:
                        print e
                        print 'waiting...'   
                        time.sleep(2)
                        continue

        print 'all machines have initialized'

        heap_thread = HeapThread()
        heap_thread.daemon = True
        heap_thread.start()

        poisson_lambda = cf.poisson_lambda
        simu_len = cf.simu_len
        get_score_pb = cf.get_score_pb
        client = ClientObject()
        client.start(poisson_lambda, simu_len, get_score_pb)

        while True:
                time.sleep(5)
