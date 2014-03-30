#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

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
import socket

tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
score_board = [[0 for x in xrange(3)] for x in xrange(3)]

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

def update_score(event_type, score):
	event_type_index = get_event_type_index(event_type)
	if event_type_index != -1:
		score_board[event_type_index] = score
		with open(s_file_name, 'r+') as s_file :
			s_file_data = s_file.readlines()
			s_file_data[event_type_index] = str(event_type) + ': ' + str(score) + '\n'
			s_file.seek(0)
			s_file.writelines(s_file_data)
		print ''
		print event_type + ':' + str(score)
	return True

class ServerThread(threading.Thread):
	"""a RPC server listening to push request from the server of the whole system"""
	def __init__(self, host_name, port):
		threading.Thread.__init__(self)

		self.host_name = host_name
		self.port = port

		if self.__init_file() == False :
			sys.exit(1)

		self.localServer = SimpleXMLRPCServer((host_name, port))
		self.localServer.register_function(update_score, "pushUpdate") # register update_score with an alias name pushUpdate
		
	def __init_file(self) :
		try :
			# initialize output files
			s_file = open('./log/score_board.out', 'w')
			s_file.writelines([var[0] + ': ' + str(list(score_board[var[1]]))+'\n' for var in sorted(event_type_dict.iteritems(), key=lambda d:d[1], reverse=False)]) 
			s_file.close()

			t_file = open('./log/tally_board.out', 'w')

			tally_board_transpose = np.transpose(tally_board)

			t_file.writelines([var[0] + ': ' + str(list(tally_board_transpose[var[1]]))+'\n' for var in sorted(team_name_dict.iteritems(), key=lambda d:d[1], reverse = False)]) 
			t_file.close()
			# end of initialize output files

		except :
			info = sys.exc_info()
			print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
			return False
		else :
			return True

	def run(self):
		self.localServer.serve_forever()

class ClientObject:
	def __init__(self, host_name, port, remote_host_name, remote_port):
		self.address = (host_name, port)
		self.remote_address = (remote_host_name, remote_port)
		self.options = {"MEDAL": self.get_medal_tally,
				"SCORE": self.get_score,
				"REGI": self.register_events,
				"DE_REGI": self.de_register_events,
				}

	def get_medal_tally(self, s, args):
		team_name = "Gauls"
		if len(args) >= 1:
			team_name = args[0]
		result = s.getMedalTally(team_name)
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

	def get_score(self, s, args):
		event_type = "Curling"
		if len(args) >= 1:
			event_type = args[0]
		result = s.getScore(event_type)

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

	def register_events(self, s, args):
		event_types = ["Curling"]
		if len(args) >= 1:
			event_types = args
		return s.registerClient(self.address[0]+":"+str(self.address[1]), event_types)

	def de_register_events(self, s, args):
		event_types = ["Curling"]
		if len(args) >= 1:
			event_types = args
		return s.deRegisterClient(self.address[0]+":"+str(self.address[1]), event_types)

	def usage(self):
		print "Usage: 'COMMAND'"
		print "COMMAND:\n\tMEDAL [TEAMNAME=Gauls]: Get medal number array [gold, silver] for the given team"
		print "\tSCORE [EVENTTYPE=Curling]: Get scores array for the given event type" 
		print "\tREGI [EVENTTYPE1=Curling [EVENTTYPE2=Skating [EVENTTYPE3=Skiing]]]: Register events for push scores" 
		print "\tDE_REGI [EVENTTYPE1=Curling [EVENTTYPE2=Skating [EVENTTYPE3=Skiing]]]: De-register events for push scores"

	def start(self):
		try:
			URL = "http://" + self.remote_address[0] + ":" + str(self.remote_address[1]);

			s = xmlrpclib.ServerProxy(URL)
		except socket.error, (value,message):
			print "Could not open socket to the server: " + message
			return
		except :
			info = sys.exc_info()
			print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
			return
		while True:
			sys.stdout.write('-> ')
			line = raw_input('')
			r = re.split(r'\s+',line) # parse input parameters (separated by blank(s))
			r_len = len(r)
			command = r[0]
		
			if r_len == 1 and '' in r: # if no parameters, show the command instruction message
				self.usage()
				continue

			if command in self.options:
				try :
					print self.options[command](s, r[1:len(r)])
				except :
					info = sys.exc_info()
					print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
					continue

if __name__ == "__main__":
	if cf.self_ip == '' :
		host_name = socket.gethostbyname(socket.gethostname()) # try to automatically find the local ip, sometimes it may fail, so I suggest to denote the local ip in the configure file.
	else :
		host_name = cf.self_ip
	port = cf.self_port

	remote_host_name = cf.server_ip
	remote_port = cf.server_port


	# run RPC server
	server = ServerThread(host_name, port)
	server.daemon = True; # allow the thread exit right after the main thread exits by keyboard interruption.
	server.start() # The server is now running
	client = ClientObject(host_name, port, remote_host_name, remote_port)
	client.start()
	# end of run RPC server

	server.join(1)

