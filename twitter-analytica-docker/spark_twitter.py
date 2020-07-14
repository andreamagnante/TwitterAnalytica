import json, pprint, requests, textwrap,time
import re
import ast
from datetime import date
import datetime 
import boto3

host = ''
session_url = host + '/sessions/'
headers = {'Content-Type': 'application/json'}
data = {'kind': 'pyspark'}

# Per ora stabilisco una semplice sessione
#session_url = 'http://ec2-3-235-64-240.compute-1.amazonaws.com:8998/sessions/3'
#r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
#session_url = host + r.headers['location']
#statements_url = session_url + '/statements'

statements_url = ""

def init_session():
	global statements_url 
	r = requests.get(session_url, headers=headers)
	
	sessions_json = r.json()
	session_id = 0
	session_found = False

	for s in sessions_json['sessions']:
		if s['state'] == 'idle' or s['state'] == 'busy':
			session_id = s['id']
			session_found = True
			statements_url = session_url + str(session_id) + '/statements'
			break


	if not session_found:
		r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
		new_session_id = r.json()['id']
		condition_start = False
		while not condition_start:
			r = requests.get(host + '/sessions/'+str(new_session_id), headers=headers)
			if r.json()['state'] == 'idle':
				statements_url = session_url + str(new_session_id) + '/statements'
				condition_start = True

	print(statements_url)

def spark_search(keyword, interval):
	init_session()
	print(keyword)

	print(interval)

	all_today_files = []

	session = boto3.Session(
    	aws_access_key_id='',
    	aws_secret_access_key='',)
	
	s3 = session.resource('s3')
	my_bucket = s3.Bucket('sapienza-b')

	tod = datetime.datetime.now()

	if interval == "Last 24h":
		date_today = tod.strftime('%Y/%m/%d')	
		print(date_today)
		for object_summary in my_bucket.objects.filter(Prefix =str(date_today)+'/'):
			all_today_files.append('\'s3a://sapienza-b/' + object_summary.key + '\'')
	elif interval == "Last 3 days":
		for i in range(3):
			d = datetime.timedelta(days = i)
			a = tod - d
			current_day = a.strftime('%Y/%m/%d')
			for object_summary in my_bucket.objects.filter(Prefix =str(current_day)+'/'):
				all_today_files.append('\'s3a://sapienza-b/' + object_summary.key + '\'')

	else:
		for i in range(7):
			d = datetime.timedelta(days = i)
			a = tod - d
			current_day = a.strftime('%Y/%m/%d')
			for object_summary in my_bucket.objects.filter(Prefix =str(current_day)+'/'):
				all_today_files.append('\'s3a://sapienza-b/' + object_summary.key + '\'')

	'''
	print("LISTA FILE\n")
	#print(all_today_files)
	for f in all_today_files:
		print(f)
	print("\n")
	'''

	parquets_path = ','.join(all_today_files)
	#print(parquets_path)

	data = {
	  'code': textwrap.dedent("""
	    from pyspark.sql import SQLContext
	    import pyspark.sql.functions as psf
	    sqlContext = SQLContext(sc)

	    df = sqlContext.read.parquet("""+parquets_path+""")
	    #print(df.count())
	    words_list = ['"""+keyword+"""']
	    result = df.filter(psf.col('text').rlike('(^|\s)(' + '|'.join(words_list) + ')(\s|$)'))
	    result_list = result.select("text").rdd.flatMap(lambda x: x).collect()
	    print(result_list)
	 	""")
	}

	print(statements_url)

	r = requests.post(statements_url, data=json.dumps(data), headers=headers)
	print(r)
	#pprint.pprint(r.json())
	#time.sleep(40)
	#r = requests.get(session_url+'/statements', headers=headers)


	k = r.json()
	id_request = k['id']
	state_request = k['state']
	#print(id_request)
	#print(state_request)

	condition = False
	query_result = {}

	while not condition:
		r = requests.get(statements_url, headers=headers) 
		#query_result = r.json()['statements'][-1]
		#pprint.pprint(query_result)
		response = r.json()
		#pprint.pprint(r.json())
		#time.sleep(50)
		#spprint.pprint(type(response['statements']))
		for x in response['statements']:
			#print (x['id'])
			if (x['id'] == id_request):
				#print(x) # Su x abbiamo la richiesta che stiamo processando
				current_state = x['state']
				print(current_state)
				if current_state == 'available':
					query_result = x
					condition = True
				else:
					time.sleep(2)
					break				
	
	#pprint.pprint(query_result)

	query_result = query_result['output']['data']
	query_result = (list(query_result.values())[0])
	query_result = query_result.split('"tweet":')
	
	return query_result


def clean_text(text):
	text = json.dumps(text)
	text = text.replace('[', '')
	text = text.replace(']', '')
	text = text.replace('{', '')
	text = text.replace('}', '')
	text = text.replace(',', '')
	text = text.replace("\\", "")
	text = text.replace("'", "")
	text = text.replace("\"", "")
	return text


def count_retweet(tweets):
	text = clean_text(tweets)
	
	data = {
	  'code': textwrap.dedent("""
	  	from operator import add
	  	textFile = sc.parallelize(['""" +text+"""'])
	  	counts = textFile.flatMap(lambda line: line.split(" ")).filter(lambda w: 'RT' in w).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1 + v2)
	  	counts.collect()
	 	""") 
	}
	r = requests.post(statements_url, data=json.dumps(data), headers=headers)
	k = r.json()
	id_request = k['id']
	state_request = k['state']

	condition = False
	query_result = {}

	while not condition:
		r = requests.get(statements_url, headers=headers) 
		response = r.json()

		for x in response['statements']:

			if (x['id'] == id_request):
				current_state = x['state']
				print(current_state)
				if current_state == 'available':
					query_result = x
					condition = True
				else:
					time.sleep(2)
					break

	query_result = query_result['output']['data']
	
	query_result = list(query_result.values())[0]
	
	dict_result = ast.literal_eval(query_result)
	print(type(dict_result))

	for el in dict_result:
		if el[0] == "RT":
			return el[1]

def count_tags(tweets):
	text = clean_text(tweets)
	data = {
	  'code': textwrap.dedent("""
	  	textFile = sc.parallelize(['""" +text+"""'])
	  	counts = textFile.flatMap(lambda line: line.split(" ")).filter(lambda w: '@' in w).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1 + v2)
	  	counts.collect()
	 	""")
	}
	r = requests.post(statements_url, data=json.dumps(data), headers=headers)

	k = r.json()
	id_request = k['id']
	state_request = k['state']

	condition = False
	query_result = {}

	while not condition:
		r = requests.get(statements_url, headers=headers) 
		response = r.json()

		for x in response['statements']:

			if (x['id'] == id_request):
				current_state = x['state']
				print(current_state)
				if current_state == 'available':
					query_result = x
					condition = True
				else:
					time.sleep(2)
					break

	query_result = query_result['output']['data']
	
	query_result = list(query_result.values())[0]
	
	dict_result = ast.literal_eval(query_result)
	#print(dict_result)
	#print(type(dict_result[0]))
	dict_result = sorted(dict_result)
	dict_result = sorted(dict_result, key=lambda x: x[1], reverse=True)

	return dict_result


def word_count(tweets):
	text = clean_text(tweets)
	
	#print(text)
	
	data = {
	  'code': textwrap.dedent("""
	  	textFile = sc.parallelize(['""" +text+"""'])
	  	counts = textFile.flatMap(lambda line: line.split(" ")).filter(lambda w: '#' in w).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1 + v2)
	  	counts.collect()
	 	""")
	}
	r = requests.post(statements_url, data=json.dumps(data), headers=headers)

	k = r.json()
	id_request = k['id']
	state_request = k['state']

	condition = False
	query_result = {}

	while not condition:
		r = requests.get(statements_url, headers=headers) 
		response = r.json()

		for x in response['statements']:

			if (x['id'] == id_request):
				current_state = x['state']
				print(current_state)
				if current_state == 'available':
					query_result = x
					condition = True
				else:
					time.sleep(2)
					break

	query_result = query_result['output']['data']
	
	query_result = list(query_result.values())[0]
	
	dict_result = ast.literal_eval(query_result)
	#print(dict_result)
	#print(type(dict_result[0]))
	dict_result = sorted(dict_result)
	dict_result = sorted(dict_result, key=lambda x: x[1], reverse=True)

	return dict_result
		