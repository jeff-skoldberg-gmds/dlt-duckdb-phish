import duckdb


local_con = duckdb.connect("new.db")
local_con.sql("ATTACH 'md:'")
local_con.sql("CREATE DATABASE ph_land_test FROM CURRENT_DATABASE()")
