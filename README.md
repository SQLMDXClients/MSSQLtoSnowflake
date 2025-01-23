# MSSQLtoSnowflake
Client code to migrate MS SQL objects using Snowflake Native Application to Snowflake.

There are many valid solutions available to pull the metadata out of MS SQL Server and push it
to Snowflake.  You can choose any tool at your disposal.   We present some simple 
python scripts here so you can see the minimal steps necessary.

Security is very important in this process.  Each process has been designed to be
isolated from the other processes.  You can see each step in detail so you have no questions.
