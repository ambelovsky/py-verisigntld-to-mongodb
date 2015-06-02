# py-verisigntld-to-mongodb
Python 2/3 script to download version TLD zone file, incrementally parse domains from it without unarchiving the full file, and load the domain data into MongoDB

Configuration
=============

Open etl.py in a text editor or IDE and modify the variables at the top of the script.


Usage
=====

After you've configured etl.py appropriately, run:

    python ./etl.py

*note: This process takes a while. Find popcorn.
