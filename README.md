# Verisign to MongoDB
Python 2/3 script to download Verisign TLD zone file, extract, transform, and load the domain data into MongoDB

Description
===========

Verisign's .com, .net, and .name top-level domain information is available in zone file format for download from
their trusted FTP server. To gain access to the FTP server(s) and top-level domain zone files, request permission
at [verisigninc.com](http://www.verisigninc.com/en_US/channel-resources/domain-registry-products/zone-file/index.xhtml).

The provided files are in gzip format. This Python script, compatible with Python versions 2 and 3, will:

1. Download gzipped files from Verisign's trusted FTP servers
2. Incrementally stream data from the gzipped files into smaller sorted temporary ASCII storage files
3. Dedupe domain names
4. Load unique domain names into a MongoDB database
5. Cleanup temporary ASCII storage files

This script was written to allow for the parsing and processing of these large files on small worker server
instances without placing heavy load on memory or storage capacity.


Configuration
=============

Open etl.py in a text editor or IDE and modify the variables at the top of the script.


Usage
=====

After you've configured etl.py appropriately, run:

    python ./etl.py

*note: This process takes a while. Find popcorn.
