import duckdb
import pandas

# Create a Pandas dataframe
my_df = pandas.DataFrame.from_dict({'a': [42]})

# create the table "my_table" from the DataFrame "my_df"
# Note: duckdb.sql connects to the default in-memory database connection

# insert into the table "my_table" from the DataFrame "my_df"
duckdb.sql(
    "CREATE TABLE my_table AS SELECT * FROM my_df;INSERT INTO my_table SELECT * FROM my_df;")

my_table = duckdb.sql("SELECT * FROM my_table").df()

print(my_table)
