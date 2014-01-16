#!/usr/bin/env python

import subprocess,optparse,os,sys

usage = "usage: python %prog [options]"
description="Script to setup area for rsmpp pipeline"
vers='1.0'

#Defines the options as present in gsm.py
parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-m", "--mode", action="store", type="string", dest="mode", default="HBA", help="Specify which version of rsmpp to setup (HBA or LBA) [default: %default]")
(options, args) = parser.parse_args()

def getfiles(mode, cwd):
	rootdir=os.path.join("/home/as24v07/scripts/rsmpp", mode)
	print "Copying scripts..."
	subprocess.call("cp {0} {1}".format(os.path.join(rootdir, "rsmpp*.py"), cwd), shell=True)
	subprocess.call("cp {0} {1}".format(os.path.join(rootdir, "rsmpp*.parset"),cwd), shell=True)
	subprocess.call("cp {0} {1}".format(os.path.join(rootdir, "to_process.py"),cwd), shell=True)
	print "Done!"
	print "Copying default parsets folder..."
	subprocess.call("cp -r {0} {1}".format(os.path.join(rootdir, "parsets"), cwd), shell=True)
	print "Done!"
	

allowed_modes=["HBA","LBA"]
dir=os.getcwd()

Mode=options.mode
if Mode not in allowed_modes:
	print "Mode must be either 'HBA' or 'LBA'"
	sys.exit()
	
print "Mode: {0}".format(Mode)
print "Current Directory: {0}".format(dir)
print "Fetching Stuff..."
getfiles(Mode, dir)
print "Stuff Fetched."
print "Testing Environment..."

if Mode=="HBA":
	subprocess.call("./rsmpp.py --version", shell=True)
else:
	subprocess.call("./rsmpp_lba.py --version", shell=True)

if os.path.isfile("emailslofar.py"):
	subprocess.call("rm emailslofar.p* quick_keys.*", shell=True)
	
print "\nCheck your parsets and run!\n"	
