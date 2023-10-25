import duckdb
import pandas as pd
# create a connection to a file called 'file.db'
con = duckdb.connect('file.db')
# create a table and load data into it
# con.sql('CREATE TABLE test(i INTEGER)')
# con.sql('INSERT INTO test VALUES (42)')
# query the table
con.register('a', pd.DataFrame([1,2,3]))
con.execute('create table b as (select * from a)')
con.table('b').show()
con.execute('select * from b;')
# explicitly close the connection
con.close()