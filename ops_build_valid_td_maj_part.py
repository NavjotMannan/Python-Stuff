# -*- coding: utf-8 -*-
import teradata
import re
import os

"""
Given Utility was written as part of Project that involved replicating data from EDW(Teradata) to Oracle(DataMart).
Around 50+ tables were validated using this simple utility. Teradata DBC tables were leveraged to avoid maintaining an input list.
I preferred functional programming instead of OOPs considering the simplicity of the use case.
Below solution is a mix of Local Code for Teradata & Remote code for Oracle(Highlighting this as these kind of use cases can arise).
Because of this - Two .py files are written instead of one.
Remote server has python2(for Oracle), while local code(for Teradata) is in python3(even such case can arise :) )
I/P: i). Queries on DBC using search string common for all EDW tables
	 ii). Oracle(SQL) query is ran on same tablenames in Oracle
O/P: Excel showcasing the matching vs non-matching tables at a point in time.
Plugin(In-progress): Email containing excel that can be scheduled at desired frequency
Files Involved: ops_build_valid_td_maj_part.py, orcl_val_cnt.py, td_f.txt, validation_result.csv,   
"""

tbl_list = []
td_list = []
database = '******'
user = "******"
host = "******"

"""Step 1: Connect to Teradata using teradata python module"""
udaExec = teradata.UdaExec (appName="OraclevsTDApp", version="1.0",
                            logConsole=False)

session = udaExec.connect(method="odbc", system="******",username="******", password="******);

"""Step 2: Retrieve all tables specific to a project and append to a list"""
for row in session.execute("select tablename from dbc.tablesv where databasename ='******' and tablename like '%************%'"):
    actl_row_data = re.search('(?<=\[)\w+', str(row))
    table_val = actl_row_data.group(0)
    actl_table_val = table_val.rsplit('_', 1)[0]
    tbl_list.append(actl_table_val)

"""Step 3: For each table retrieved in Step1, add count to create a list of tuples"""
for tbl in  tbl_list:
    print(tbl)
    print("select count(*) from {}.{}_cur".format(database, tbl))
    for val in session.execute("select count(*) from {}.{}_cur".format(database, tbl)):
        count_val_data = re.search('(?<=\[)\d+', str(val))
        cnt_val = count_val_data.group(0)
        td_list.append((tbl, cnt_val))

print(td_list)

session.close()

"""Step 4: Write content of tuples to a file. This could have been combined with Step3"""

with open("td_f.txt",'w') as td_f:
    for i,j in td_list:
        print(i+","+j,file=td_f)

"""Step 5: Sftp'ing file in Step 4 to remote server as Oracle connection was not possible remotely'"""

os.system("sftp -b td_batch_data.txt *****@******")
"""Step 6: Executing Oracle Table:Cnt generation code for comparison on Remote host'"""
os.system("""ssh *****@****** <<eof
/usr/bin/python2.7 orcl_val_cnt.py
eof
""")
"""Step 7: Sftp'ing file in Step 6 to local server(for ease or avoid logging server periodically). Email functionality to be added'"""
os.system("sftp -b get_batch_val_result.txt *****@******")
