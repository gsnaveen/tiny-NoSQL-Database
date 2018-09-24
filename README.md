## tiny-NoSQL-Database

As part of a coding challange the solution was to be in memory, with inserts & updates to the structure. I ended up with HASH of HASHES solution. The driving key was the customer_id.

the first approach was to create a program which to add + update the records in a HASH.

(No error checking is done at this moment. I am excited in putting components together and then make it industrial strength)

#### Steps done so far:

1) Web Server (so that multiple processes can make a call to update the memory)
2) A process can update and retrive data based on the key
3) Logging    (Since this is just in memory, added logs for every transaction call. with Key/customer_id and timestamp as the filename)
4) Restart the application with the merged transaction logs when needed. python ./noSQL.py StartFromBackup # Start with processed transactions
5) Master backup file with incremental updates from transactions.

#### Next steps:
1) Write a program to merge the transaction logs.

	a) One file per user.
	
	b) Sequential user to file mapping. File size configurable to be 128--256MB.
	
	c) Take into consideration deletion of data.

#### Steps to be done for Scaling:
1) Apache Load balancer (to be based on hash of key to keep it simple)
2) Memory allocation and optimization
3) Replication in case of a Node failure
4) Consistent hashing

#### Steps to execute/start the program:

python ./noSQL.py #main Program

python ./noSQL.py StartFromBackup # Start with processed transactions

python ./noSQL_ProcessTransaction_logs.py #Combining transactions logs into one serialized file (work in progress)

#### Libraries used

from flask import Flask, jsonify, request

import json,time,os,sys,shutil,ast,pickle

from datetime import datetime