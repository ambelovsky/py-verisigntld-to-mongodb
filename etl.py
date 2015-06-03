import gzip, struct, shutil
import sys, io, os, re

import pymongo
from pymongo import ASCENDING, DESCENDING

from ftplib import FTP

# Configuration
ftp_user = ""
ftp_pass = ""

mongo_conf = {
    'host': '127.0.0.1',
    'port': 27017
}

# ftp servers and files that can be found on each for processing
files = {
    'rzname.verisign-grs.com': [
        {
            'zone_file': 'master.name.zone.gz',
            'zone_extension': 'name',
            'zone_type': 2 # name files use a different zone file format
        }
    ],
    'rz.verisign-grs.com': [
        {
            'zone_file': 'com.zone.gz',
            'zone_extension': 'com',
            'zone_type': 1
        },
        {
            'zone_file': 'net.zone.gz',
            'zone_extension': 'net',
            'zone_type': 1
        }
    ]
}

enable_size_check = False # Provides accurate completion estimates but takes longer to process

# Global vars
domains = set()
curr_ext = ""
curr_zone_type = 1
data_dir = './data'
bad_start = ['$', ';', ' ', 'NS ']
domain_regex = re.compile("[a-zA-Z\d-]{,63}(\.[a-zA-Z\d-]{,63})*")

# Init Steps
def remake_directory(dir):
    """ Create directory if it doesn't exist. """
    shutil.rmtree(dir)
    os.makedirs(dir)

# Retreive File
def fetch_file(server, file_config):
    """ Retreives the proper zone file from the FTP server. """
    print("Fetching file: %s" % file_config['zone_file'])
    
    # Prevents connecting to Verisign if the file is already in the directory
    if os.path.exists(file_config['zone_file']): return
    
    with FTP(server, ftp_user, ftp_pass) as ftp:
        ftp.retrbinary("RETR " + file_config['zone_file'], open(file_config['zone_file'], 'wb').write)

def discard_file(server, file_config):
    """ NOT YET IMPLEMENTED """
    pass

# Compressed File Processing
def process_file(path):
    print("Processing file: %s" % path)
    file_size = 0
    bytes_processed = 0
    
    if enable_size_check:
        with gzip.open(path, 'r') as gzfile:
            stdcount = 0
            for line in gzfile:
                file_size += len(line)
                if stdcount > 250000:
                    sys.stdout.write("\rChecking size: %d MB" % (file_size / 1000000))
                    sys.stdout.flush()
                    stdcount = 0
                stdcount += 1
            print("...done.")
    
    with gzip.open(path, 'r') as gzfile:
        stdcount = 0
        # stream extracted info
        for line in gzfile:
            process_line(line)
            if(len(domains) > 100): lines_to_disk(domains)
            bytes_processed += len(line)
            
            if stdcount > 250000:
                if enable_size_check:
                    sys.stdout.write("\rProcessing: %d%%" % (bytes_processed/file_size*100))
                    sys.stdout.flush()
                else:
                    sys.stdout.write("\rProcessed: %d MB" % (bytes_processed / 1000000))
                    sys.stdout.flush()
                stdcount = 0
            stdcount += 1
        lines_to_disk(domains)
        print("...done.")

# Line Processing
def lines_to_disk(lines):
    """ Caches lines on disk while processing. """
    for line in lines:
        with open(data_dir + '/' + line[:2] + '.dat', mode='a') as file:
            file.write(line + "\n")
    lines.clear()

def process_line(line):
    """ Checks and extracts domain information from zone file lines. """
    line = line.decode('ascii').strip().lower()
    
    if check_line(line) == False: return
    domain = extract_domain(line)
    
    # Basic deduplication by storing domains in set
    domains.add(domain)

def check_line(line):
    """ Checks a zone file line. """
    # check to be sure line doesn't begin with non-domain line prefix
    if line[:1] in bad_start:
        return False
    
    # make sure the line matches the format for a domain line
    line_parts = line.split()

    if type(line_parts) != list: return False
    if curr_zone_type == 1:
        # DOMAIN NS ns.domain.ext
        if len(line_parts) < 3: return False
        if line_parts[1] != 'NS': return False
        if not domain_regex.match(line_parts[2]): return False
    else:
        # domain.name.	10800	in	ns	ns.domain.ext.
        if len(line_parts) < 5: return False
        if line_parts[3] != 'ns': return False
        if not domain_regex.match(line_parts[4]): return False
        if len(line_parts[0].split('.')) < 3: return False
    
    return True

def extract_domain(line):
    """ Extracts the domain name part from a checked zone file line. """
    if curr_zone_type == 1: return line.split()[0]
    else: return line.split()[0].split('.')[-3]

# Stored Line Processing
def process_lines_on_disk():
    """ Final dedupe and commit of cached line information. """
    print("Processing data directory...")
    files = os.listdir(data_dir)
    
    for file in files:
        with open(data_dir + '/' + file, 'r') as content_file:
            content = content_file.read().strip()
        domains = set(content.split("\n"))
        commit(domains)

def commit(domains):
    """ Commit domain names to their destination. """
    print("Committing domains to database...")
    with pymongo.MongoClient(mongo_conf['host'], mongo_conf['port']) as client:
        db = pymongo.database.Database(client, 'dns')
        collection = db['domains']
        collection.insert_many([{'ext': curr_ext, 'name': domain} for domain in domains])
    domains.clear()

# Database configuration
def db_config():
    print("Configuring database...")
    with pymongo.MongoClient(mongo_conf['host'], mongo_conf['port']) as client:
        db = pymongo.database.Database(client, 'dns')
        collection = db['domains']
        collection.drop()
        collection.create_index([("ext", ASCENDING), ("name", ASCENDING)],
            unique = True,
            background = True,
            sparse = True
        )

# Main Script Execution
db_config()
for server in files:
    for file in files[server]:
        bad_start.append(file['zone_extension'].upper() + '.')
        curr_ext = file['zone_extension'].lower()
        curr_zone_type = file['zone_type']
        
        fetch_file(server, file)
        remake_directory(data_dir)
        process_file(file['zone_file'])
        discard_file(server, file)
        process_lines_on_disk()
        
        bad_start.pop()
        curr_ext = ""

# Cleanup
remake_directory(data_dir)
