#!/bin/env python

import logging
import paramiko
from subprocess import Popen, PIPE
from os.path import join

HOST = '10.1.30.34'  # Natanya
USER = 'root'
KEY = '~/.ssh/id_rsa'
JSON_DIRS = "/data1/"
JSON_DST = "/share/host"
MAX_BANDWIDTH = 2*1024  # in KB
LOG = "/tmp/dripper.log"

logging.basicConfig(
    filename=LOG, 
    format="%(asctime)s %(name)s %(levelname)s: %(message)s", 
    level=logging.DEBUG)


def drip_rsync(src, dst):
    """
    Starts rsync between src and dst and waits until done,
    returning the output of the command.
    """
    bw = "--bwlimit=%d" % MAX_BANDWIDTH
    args = ['rsync', '-az', '-b', bw, src, dst]
    p = Popen(args, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    return p.communicate()


def connect():
    """
    Connects to the remote server using paramiko and
    returns a transport object along an sftp one.
    The transport object needs to be closed.
    """
    paramiko.util.log_to_file(LOG)
    trans = paramiko.Transport((HOST, 22))
    rsa_key = paramiko.RSAKey.from_private_key_file(KEY)
    trans.connect(username=USER, pkey=rsa_key)
    sftp = paramiko.SFTPClient.from_transport(trans)
    
    return trans, sftp


def drip():
    """
    Copies over each directory of JSONs from the host
    to the local path, probably /share/host, while
    modifying the destination with a 'acs_' prefix.
    """
    log = logging.getLogger('dripper')
    t, stfp = connect()
    dirs = sftp.listdir(JSON_DIRS)
    del sftp
    del t
    for json_dir in [d for d in dirs if d.isdigit()]:
        src = "%s/" % join(JSON_DIRS, json_dir)
        dst = join(JSON_DST, 'acs_%s/' % json_dir)
        drip_rsync(src, dst)


