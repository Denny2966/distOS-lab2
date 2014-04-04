#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import time
import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
#import SimpleXMLRPCServer
import os
import sys
import socket
import xmlrpclib
import frontend_config as cf
import re
import numpy as np

win_per_num_request = cf.win_per_num_request
import heapq as hp
MAX_HEAP_SIZE = 1000
heap = []
c_time = 0
heap_size = 0
remove_count = 0

ack_num_dict = {}

process_num = 0

heap_lock = threading.Lock()
dict_lock = threading.Lock()

pid = cf.process_id
cluster_info = cf.cluster_info

myipAddress = cluster_info[str(pid)][0]
myport = cluster_info[str(pid)][1]

all_processes = []

global client_object
# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
score_board = [[0 for x in xrange(4)] for x in xrange(3)]

team_name_dict = {"Gauls":0, "Romans":1}
medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}

t_file = None
s_file = None
l_file = None
w_file = None

t_file_name = './log/tally_board.out'
s_file_name = './log/score_board.out'
l_file_name = './log/event_with_l_clock.out'
w_file_name = './log/winners_list.out'

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

class ClientObject:
    def __init__(self, remote_host_name, remote_port):
        self.remote_address = (remote_host_name, remote_port)
        
        URL = "http://" + self.remote_address[0] + ":" + str(self.remote_address[1])
        self.s = xmlrpclib.ServerProxy(URL)

    def get_medal_tally(self, team_name = 'Gauls'):
        result = self.s.getMedalTally(team_name)
    #		team_name_index = get_team_name_index(team_name)
    #
    #		if team_name_index != -1 :
    #			tally_board[team_name_index] = result
    #
    #			# write obtained medal tally into the output file
    #			with open(t_file_name, 'r+') as t_file :
    #				t_file_data = t_file.readlines()
    #				t_file_data[team_name_index] = str(team_name) + ': ' + str(result) + '\n'
    #				t_file.seek(0)
    #				t_file.writelines(t_file_data)

        return result

    def get_score(self, event_type = 'Curling'):
        result = self.s.getScore(event_type)

    #		event_type_index = get_event_type_index(event_type)
    #		if event_type_index != -1:
    #			score_board[event_type_index] = result
    #
    #			# write obtained scores into the output file
    #			with open(s_file_name, 'r+') as s_file :
    #				s_file_data = s_file.readlines()
    #				s_file_data[event_type_index] = str(event_type) + ': ' + str(result)  + '\n'
    #				s_file.seek(0)
    #				s_file.writelines(s_file_data)
        return result

    def incrementMedalTally(self, teamName, medalType):
        result = self.s.incrementMedalTally(teamName, medalType)
        return result

    def setScore(self, eventType, score): # score is a list (score_of_Gauls, score_of_Romans, flag_whether_the_event_is_over)
        return self.s.setScore(eventType, score)

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
#            except:
#                pass

    heap_lock.release()
    return flag

def send_ack(l_time):
    global ack_num_dict
    global dict_lock

    l_time = tuple(l_time)

    dict_lock.acquire()
    if l_time in ack_num_dict:
        ack_num_dict[l_time] += 1
    else:
        ack_num_dict[l_time] = 1
    dict_lock.release()
    return True

def check_alive():
    return True

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
        global remove_count
        global win_per_num_request

        have_sent_ack = set()
        ExpireCount = 3 # 3 seconds
        flag_count = 0

        while True:
            heap_lock.acquire()
            if heap_size == 0:
                print 'nothing'
                pass
            else:
                time.sleep(0.1)
#                print 'sent ack list: ', list(have_sent_ack)
                for l_time in heap:
                    if l_time not in have_sent_ack:
                        have_sent_ack.add(l_time)
                        for s in s_list:
                            try:
                                s.send_ack(l_time)
                            except Exception as e:
                                print 'send ack error: ', e
                                pass
                dict_lock.acquire()
                try:
                    print '++++', heap[0]
                    print '++++', ack_num_dict[heap[0]]
                    while heap_size > 0 and heap[0] in ack_num_dict and ack_num_dict[heap[0]] >= process_num:
                        del ack_num_dict[heap[0]]
                        ele = hp.heappop(heap)
                        print '----', ele
                        remove_count += 1
                        heap_size -= 1
                        flag_count = 0
                        with open(l_file_name, 'a') as l_file :
                            l_file.write(str(ele) + '\n')
                        if remove_count % win_per_num_request == 0:
                            with open(w_file_name, 'a') as w_file :
                                w_file.write(str(ele[1]) + '\n')
                except Exception as e:
                    print e
                dict_lock.release()
            heap_lock.release()
            print 'heap thread'
            time.sleep(1+np.random.rand()*2)

if __name__ == "__main__":
    try:
        l_file = open(l_file_name, 'w')
        l_file.close()
        w_file = open(w_file_name, 'w')
        w_file.close()
    except Exception as e:
        print e
        sys.exit(1)

    for i in cluster_info:
        all_processes.append((cluster_info[i][0], cluster_info[i][1], int(i)))
    process_num = len(all_processes)

    s_list = []
    for process in all_processes:
        URL = "http://" + process[0] + ":"+ str( process[1] )
        print URL
        s_list.append(xmlrpclib.ServerProxy(URL))
    
    remote_host_name = cf.server_ip
    remote_port = cf.server_port
    client_object = ClientObject(remote_host_name, remote_port)

    heap_thread = HeapThread()
    heap_thread.daemon = True
    heap_thread.start()

#    server = SimpleXMLRPCServer(('', myport))	
    server = AsyncXMLRPCServer(('', myport), SimpleXMLRPCRequestHandler)

    try:
        server.register_instance(ClientObject(remote_host_name, remote_port))
        server.register_function(record_request, 'record_request')
        server.register_function(check_alive, 'check_alive')
        server.register_function(send_ack, 'send_ack')
    except socket.error, (value,message):
        print "Could not open socket to the server: " + message
        sys.exit(1)
    except :
        info = sys.exc_info()
        print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
        sys.exit(1)

    # run
    server.serve_forever()
