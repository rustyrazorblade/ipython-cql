This extension is based on the work done by Catherine Devlin: https://github.com/catherinedevlin/ipython-sql

For now it automatically connects to localhost.

Load the extension:
    
    %load_ext cql
    
Select a keyspace:

    %keyspace tutorial

List tables:
    
    %tables
    
Execute CQL Statements

    %cql select * from user;



