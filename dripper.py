#!/bin/env python
"""
JSON Dripper

Connects to HOST and downloads all folders from JSON_DIRS
at the speed defined by MAX_BANDWIDTH.
The local destination folder is added a prefix of acs_.
It checks that the remote dir is a number (000, 001...).

Requites ssh key added to remote host.
"""
import logging
import paramiko
from subprocess import Popen, PIPE
from os.path import join, expanduser

HOST = '10.1.30.34'  # Natanya
USER = 'root'
KEY = join(expanduser('~'), ".ssh/id_rsa")
JSON_DIRS = "/data1/dripper_in/04-07-2017/000/"
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
    log.debug("Starting dripping from all remote dirs.")
    t, sftp = connect()
    dirs = sftp.listdir(JSON_DIRS)
    del sftp
    del t
    log.debug("Connected and got dir names %s." % str(dirs))
    for json_dir in [d for d in dirs if d.isdigit()]:
        src = "%s/" % join(JSON_DIRS, json_dir)
        dst = "%s/" % join(JSON_DST, 'acs_%s' % json_dir)
        log.debug("Downloading %s to %s at rate %d KB/s." % (src, dst, MAX_BANDWIDTH))
        drip_rsync(src, dst)
        log.debug("Finished with remote %s." % src)


def main():
    while True:
        drip()
        

if __name__ == '__main__':
    main()
