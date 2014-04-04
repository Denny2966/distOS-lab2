process_id = 6 # client starts from 3. 1, and 2 are frontend servers

cluster_info = {
        '1':('127.0.0.1', 8005,),
        '2':('127.0.0.1', 8006,),

        '3':('127.0.0.1', 8100,),
		'4':('127.0.0.1', 8101,),
		'5':('127.0.0.1', 8102,),
		'6':('127.0.0.1', 8103,)
		}

poisson_lambda = 5
simu_len = 60
get_score_pb = 0.8
