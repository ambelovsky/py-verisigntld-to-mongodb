import tarfile, shutil
import io, os, re

import pymongo
from pymongo import ASCENDING, DESCENDING

from ftplib import FTP

# Configuration

# ftp servers and files that can be found on each for processing
files = {
    'rz.verisign-grs.com': [
        {
            'zone_file': 'com.zone.tgz',
            'zone_extension': 'com'
        },
        {
            'zone_file': 'net.zone.tgz',
            'zone_extension': 'net'
        }
    ],
    'rzname.verisign-grs.com': [
        {
            'zone_file': 'name.zone.tgz',
            'zone_extension': 'name'
        }
    ]
}

ftp_user = ""
ftp_pass = ""

mongo_conf = {
    'host': '127.0.0.1', #'localhost',
    'port': 27017
}

# Global vars
domains = set()
curr_ext = ""
data_dir = './data'
bad_start = ['$', ';', ' ', 'NS ']
domain_regex = re.compile("[a-zA-Z\d-]{,63}(\.[a-zA-Z\d-]{,63})*")

# Init Steps
def remake_directory(dir):
    """ Create directory if it doesn't exist. """
    if os.path.exists(os.path.dirname(dir)):
        shutil.rmtree(dir)
    os.makedirs(dir)

# Retreive File
def fetch_file(server, file_config):
    """ Retreives the proper zone file from the FTP server. """
    with FTP(server, ftp_user, ftp_pass) as ftp:
        ftp.login()
        ftp.retrbinary("RETR " + file_config['zone_file'], open(file_config['zone_file'], 'wb').write)
        ftp.quit()

def discard_file(server, file_config):
    """ NOT YET IMPLEMENTED """
    pass

# Compressed File Processing
def process_file(path):
    with tarfile.open(path, 'r|gz') as tfile:
        for entry in tfile:
            # avoid metadata files
            if '_' in entry.name:
                continue
        
            # stream extracted info
            file_obj = tfile.extractfile(entry)
            for line in file_obj:
                process_line(line)
                if(len(domains) > 100): lines_to_disk(domains)
            lines_to_disk(domains)

# Line Processing
def lines_to_disk(lines):
    """ Caches lines on disk while processing. """
    for line in lines:
        with open(data_dir + '/' + line[:2] + '.dat', mode='a') as file:
            file.write(line)

def process_line(line):
    """ Checks and extracts domain information from zone file lines. """
    if not check_line(line):
        return
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
    if type(line_parts) != list or len(line_parts) < 3: return False
    if line_parts[1] != 'NS': return False
    if not domain_regex.match(line_parts[2]): return False
    
    return True

def extract_domain(line):
    """ Extracts the domain name part from a checked zone file line. """
    return line.split()[0]

# Stored Line Processing
def process_lines_on_disk():
    """ Final dedupe and commit of cached line information. """
    files = os.listdir(data_dir)
    
    for file in files:
        with open(data_dir + '/' + file, 'r') as content_file:
            content = content_file.read()
        domains = set(content.split("\n"))
        commit(domains)

def commit(domains):
    """ Commit domain names to their destination. """
    with pymongo.MongoClient(mongo_conf['host'], mongo_conf['port']) as client:
        db = pymongo.database.Database(client, 'dns')
        collection = db['domains']
        for domain in domains:
            collection.insert_many([{'ext': curr_ext, 'name': domain} for domain in domains])
    domains.clear()

# Database configuration
def db_config():
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
remake_directory(data_dir)
db_config()
for server in files:
    for file in files[server]:
        bad_start.append(file['zone_extension'].upper() + '.')
        curr_ext = file['zone_extension'].lower()
        
        fetch_file(server, file)
        process_file(file['zone_file'])
        discard_file(server, file)
        process_lines_on_disk()
        
        bad_start.pop()
        curr_ext = ""

