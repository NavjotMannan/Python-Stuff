# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import shlex
import csv, os
import re

os.system('rm -f validation_result.csv')

orcl_lst = []

uid = "*****"
pwd = "******"
host = "*****"
port = *****
sid = '*****'

"""Step 1: Connect to Oracle using shlex, subprocess(Popen) python modules"""
def run_sql(in_query):
	header_str = 'set head off \n set feedback off \n set linesize 10000 \n set long 99999 \n set pagesize 10000 \n set serveroutput on size 100000 \n WHENEVER SQLERROR EXIT SQL.SQLCODE;\n'
	end_str = '\n EXIT SQL.SQLCODE;'
	query_path = "orcl_query.txt"
	with open(query_path, 'w') as temp_file_obj:
		temp_file_obj.write(header_str+in_query+end_str)
		qc_login = "%s/\"%s\"@%s:%d/%s"% (uid,pwd,host,port,sid)
		qc_r_login = re.sub('\"',r'\"',qc_login )
		print(qc_login)
		print(qc_r_login)
		cmd = "sqlplus -s -L %s @%s"%(qc_r_login, query_path)
		print(cmd)
		args_for_popen = shlex.split(cmd)
		print(args_for_popen)
		session = Popen(args_for_popen, stdin=PIPE, stdout=PIPE, stderr=PIPE)
		session.stdin.write(in_query)
		query_output,query_error = session.communicate()
		return query_output

"""Step 2: For all tables in Teradata, Retrieve table followed by count in Oracle. Only schema name(db) changes"""

with open("td_f.txt",'r') as td_f:
    csv_reader = dict(csv.reader(td_f, delimiter=","))

	for tbl in csv_reader:
		in_query = "select count(*) from db.%s;"%(tbl)
		print(in_query)
		cnt_val = run_sql(in_query)
		print("Value is"+str(cnt_val))
		cnt_final_val = re.findall('\d+(?=[\n])', str(cnt_val))
		print("Final Value is"+str(cnt_final_val))
		cnt_total_final_val = "".join(cnt_final_val)
		orcl_lst.append((tbl,cnt_total_final_val))
	print(orcl_lst)

orcl_dict = dict(orcl_lst)

"""Step 3: Validation Block to compare Teradata vs Oracle Tables> Writing to a file that will be sftp'ed to local'"""

with open ("validation_result.csv", 'a') as val_file:
	val_file.write("Oracle_Table"+","+"Oracle_Count"+","+"TD_Table"+","+"TD_Count"+","+"Match?")
	val_file.write("\n")
	for i in orcl_dict.items():
		for j in csv_reader.items():
			if i[0] == j[0]:	# For matching Key, if value is matching or not?
				if i[1] == j[1]:	# Value matches in this case
					val = "match"
					val_file.write(i[0] +"," + i[1] +"," +j[0] + "," + j[1] +","+val)
					val_file.write("\n")
				else:
					val = "non-match"	# Value is not matching in this case
					val_file.write(i[0] +"," + i[1] +"," +j[0] + "," + j[1] +","+val)
					val_file.write("\n")