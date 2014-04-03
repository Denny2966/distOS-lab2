import threading
import time
class test(threading.Thread):
	def run(self):
		time.sleep(1)

x = test()
x.start()
print x.isAlive()
x.join()
print x.isAlive()
