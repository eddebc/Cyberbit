#!/bin/env python
"""
JSON Dripper

Connects to HOST and downloads all folders from JSON_DIRS
at the speed defined by MAX_BANDWIDTH.
The prefix acs_ is added to the local destination folder.
It checks that the remote dir is a number (000, 001...).

Requites ssh pub key added to remote host.
"""
import logging
import paramiko
from pwd import getpwnam
from subprocess import Popen, PIPE
from os import walk, chown
from os.path import join, expanduser

HOST = '10.1.30.34'  # Natanya
USER = 'root'
KEY = expanduser("~/.ssh/id_rsa")
JSON_DIRS = "/data1/dripper_in/04-07-2017/000/"
JSON_DST = "/share/host"
MAX_BANDWIDTH = 12*1024  # in KB
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
    args = ['rsync', '-az', bw, '%s:%s' % (HOST, src), dst]
    p = Popen(args, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    #print ' '.join(args)
    #p = Popen(' '.join(args), shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    return p.communicate()


def chown_recursive(path, username):
    """
    Changes ownership of all files under path
    to username:username (his own group) but does
    not go deeper than one level.
    """
    user = getpwnam(username)
    uid, gid = user.pw_uid, user.pw_gid
    chown(path, uid, gid)
    for _, _, files in walk(path):
        for f in files:
            try:
                chown(f, uid, gid)
            except OSError:  # Files are yanked from under our feet
                pass


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
        out, err = drip_rsync(src, dst)
        if err:
            log.error("Error rsyncing %s. %s" % (src, err))
        log.debug("Finished with remote %s. %s." % (src, out))
        chown_recursive(dst, 'apd')


def main():
    while True:
        drip()
        

if __name__ == '__main__':
    main()
