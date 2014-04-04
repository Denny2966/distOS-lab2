import os
import sys
import time
sys.path.append(os.getcwd())
import timeServer.timeServer as ts
import time_config as tcf

if __name__ == '__main__':
	ts.SetupServer()
	time.sleep(10)
	if not tcf.isMaster:
		print ts.getOffset()
