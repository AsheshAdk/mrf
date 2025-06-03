# Read and display the contents of the uploaded SQL file to understand the table structure.
file_path = "/media/ashesh/Documents/Zakipoint/mrf/Task5/SQL/task/table.sql"

with open(file_path, "r") as file:
    sql_contents = file.read()

sql_contents[:2000]  # show first 2000 characters for initial inspection
