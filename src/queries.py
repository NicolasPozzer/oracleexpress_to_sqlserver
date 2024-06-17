#Queries oracle


#Queries sqlserver
create_mirror_table = """
SELECT *
INTO mirror_table
FROM main_table;
"""

select_mirror_table = "SELECT * FROM mirror_table"
select_main_table = "SELECT * FROM main_table"