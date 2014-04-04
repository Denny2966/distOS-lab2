process_id = 3 # client starts from 3. 1, and 2 are frontend servers

cluster_info = {
        '1':('127.0.0.1', 8005,),
        '2':('127.0.0.1', 8006,),

        '3':('127.0.0.1', 8100,),
		'4':('127.0.0.1', 8101,),
		'5':('127.0.0.1', 8102,),
		'6':('127.0.0.1', 8103,)
		}

poisson_lambda = 20 # number of request per second
simu_len = 60 # last time of the experiment, in unit of second
get_score_pb = 0.8
