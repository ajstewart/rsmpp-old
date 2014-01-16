#!/usr/bin/env python

#rsmpp.py

#Run 'python rsmpp.py -h' for full details and usage.

#LOFAR RSM data processing script designed for the Southampton lofar machines.

#A full user guide can be found on google docs here:
#https://docs.google.com/document/d/1IWtL0Cv-x5Y5I_tut4wY2jq7M1q8DJjkUg3mCWL0r4E

#Written by Adam Stewart, Last Update February 2013

#---Version 1.3---
#Proper logging implemented.
#Able to select baselines to image out to and will UVmax parameter.
#No mask option

#---Version 1.23---
#Changed mask creation in imaging.
#Able to select the first number of initial iterations in imaging.
#Now obsids able to select a range as well.

#---Version 1.22---
#Bug in switching to AWimager envrionment - now fixed
#AWimager now 'knows' the max baselines
#Updated check for newer build.

#---Version 1.21---
#Added option to skip to_process.py and enter a manual short list of obsids to processes.
#Updated check for newer build.

#---Version 1.2---
#AWimager threshold is now set automatically.
#New RSM environments are checked for, and will only run with correct environment.

#---Version 1.12---
#Fixed odd even bug properly.
#Deals with naming .MS measurement with .dppp

#---Version 1.11---
#Fixed odd even bug - However!! Still perahps needs to be thought about, works if the first observation listed is even.

#---Version 1.1---
#Added auto generation of calibrator skymodel.
#Added auto creation of mosaics if chosen.
#Added destroy mode to delete most of output.
#Able to chose number of bands and sub bands in bands.
#Fixed monitor user bug.

#---Version 1.0---

import subprocess, multiprocessing, os, glob, optparse, sys, datetime, string, getpass, time, logging
from functools import partial
from multiprocessing import Pool
import pyrap.tables as pt
from itertools import izip
subprocess.call(["cp", "/home/as24v07/scripts/emailslofar.py", "/home/as24v07/scripts/quick_keys.py", "."])
import emailslofar as em
import rsmpp_funcs as rsmf
import numpy as np
vers="1.3"

#Check environment
curr_env=os.environ
if curr_env["LOFARROOT"] != rsmf.correct_lofarroot:
	print "\nCorrect Environment, as of 28/02/2013, has not been loaded!\n\
Please run the following commands and then retry:\n\n\
. /opt/soft/reset-paths.sh\n\
. /opt/rsm-mainline/init-lofar.sh\n\n\
It may be the case that the build has been updated since last rsmpp version."
	sys.exit()

#Few date things for naming and user
user=getpass.getuser()
now=datetime.datetime.utcnow()
try:
	emacc=em.load_account_settings_from_file("/home/as24v07/.slofarpp/email_acc")
	known_users=em.known_users
	user_address=known_users[user]
	mail=True
except:
	mail=False

date_time_start=now.strftime("%d-%b-%Y %H:%M:%S")
newdirname="rsmpp_{0}".format(now.strftime("%H:%M:%S_%d-%b-%Y"))
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Optparse and linking to parameters + checks
#----------------------------------------------------------------------------------------------------------------------------------------------
usage = "usage: python %prog [options]"
description="This script has been written to act as a pipeline for RSM data, which is processed using a HBA MSSS style method. All parsets should be placed in a 'parsets' directory in the \
working area, and the to_process.py script is required which specifies the list of observations or snapshots to process.\n\
For full details on how to run the script, see the user manual here: https://docs.google.com/document/d/1IWtL0Cv-x5Y5I_tut4wY2jq7M1q8DJjkUg3mCWL0r4E"
parser = optparse.OptionParser(usage=usage,version="%prog v{0}".format(vers), description=description)

group = optparse.OptionGroup(parser, "General Options")
group.add_option("--obsids", action="store", type="string", dest="obsids", default="to_process.py", help="Use this to bypass using to_process.py, manually list the ObsIds you want to run in the format\
'L81111,L81112,L81113,L81114,...' (No spaces!) [default: %default]")
group.add_option("--monitor", action="store_true", dest="monitor", default=False, help="Turn on system monitoring [default: %default]")
group.add_option("-D", "--destroy", action="store_true", dest="destroy", default=False,help="Use this option to delete all the output except images, logs and plots [default: %default]")
group.add_option("-f", "--flag", action="store_true", dest="autoflag", default=False,help="Use this option to use autoflagging in processing [default: %default]")
group.add_option("-i", "--image", action="store",type="string", dest="image", default="None",help="Use this option to image the results with AWimager - 'AW' or Casa - 'CASA', enter settings in a textfile \
named 'aw.parset' (do not include ms= or image=) or 'casa.parset with just a list of image settings (do not include vis or imagename), or of course you could enter 'BOTH'... [default: %default]")
group.add_option("-I", "--initialiter", action="store", type="int", dest="initialiter", default=500,help="Define how many cleaning iterations should be performed in order to estimate the threshold [default: %default]")
group.add_option("-l", "--maxbaseline", action="store", type="int", dest="maxbaseline", default=6000,help="Enter the maximum baseline to image out to (IN m) [default: %default]")
group.add_option("--nomask", action="store_true", dest="nomask", default=False, help="Use option to NOT use a mask when cleaning [default: %default]")
group.add_option("-m", "--mosaic", action="store_true", dest="mosaic", default=False,help="Use option to produce snapshot, band, mosaics after imaging [default: %default]")
group.add_option("-M", "--move", action="store", type="string", dest="move", default="!",help="Specify the path of where output should be moved to [default: {0}]".format(os.getcwd()+"/"))
group.add_option("-n", "--nobservations", action="store", type="int", dest="nobservations", default=4, help="Specify the number of observations to process simultaneously (i.e. the number of threads)[default: %default]")
group.add_option("-j", "--target_oddeven", action="store", type="string", dest="target_oddeven", default="odd",help="Specify whether the targets are the odd numbered observations or the even [default: %default]")
group.add_option("-o", "--output", action="store", type="string", dest="newdir", default=newdirname,help="Specify name of the directoy that the output will be stored in [default: %default]")
group.add_option("-p", "--peeling", action="store", type="int", dest="peeling", default=0,help="Use this option to enable peeling, specifying how many sources to peel [default: %default]")
group.add_option("-v", "--peelingshort", action="store_true", dest="peelingshort", default=False,help="Use this option to skip the last section of the peeling procedure and NOT add back in the peeled sources [default: %default]")
group.add_option("-c", "--peelsources", action="store", type="string", dest="peelsources", default="0",help="Use this option to specify which sources to peel instead of the code taking the X brightest sources. Enter in the format\
 source1,source2,source3,.... [default: None]")
group.add_option("-q", "--loglevel", action="store", type="string", dest="loglevel", default="INFO",help="Use this option to set the print out log level ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'] [default: %default]")
group.add_option("-t", "--postcut", action="store", type="int", dest="postcut", default=0,help="Use this option to enable post-bbs flagging, specifying the cut level [default: %default]")
#This option perhaps redundant
group.add_option("-u", "--flaguser", action="store", type="string", dest="flaguser", default="",help="Give specific stations to flag out, enter in the format of: !CS001LBA;!CS002LBA; (MUST end with ;) [default: None]")
group.add_option("-w", "--overwrite", action="store_true", dest="overwrite", default=False,help="Use this option to overwrite output directory if it already exists [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Parset Options:")
group.add_option("-a", "--calparset", action="store", type="string", dest="calparset", default="parsets/cal.parset",help="Specify bbs parset to use on calibrator calibration [default: %default]")
group.add_option("-e", "--calmodel", action="store", type="string", dest="calmodel", default="AUTO",help="Specify a calibrator skymodel. By default the calibrator will be \
detected and the respective model will be automatically fetched [default: %default]")
group.add_option("-g", "--corparset", action="store", type="string", dest="corparset", default="parsets/correct.parset",help="Specify bbs parset to use on gain transfer to target [default: %default]")
group.add_option("-k", "--ndppp", action="store", type="string", dest="ndppp", default="parsets/ndppp.1.initial.parset",help="Specify the template initial NDPPP file to use [default: %default]")
group.add_option("-s", "--skymodel", action="store", type="string", dest="skymodel", default="AUTO",help="Specify a particular field skymodel to use for the phase only calibration, by default the skymodels will be\
automatically generated.[default: %default]")
group.add_option("-r", "--skyradius", action="store", type="float", dest="skyradius", default=5, help="Radius of automatically generated field model [default: %default]")
group.add_option("-y", "--dummymodel", action="store", type="string", dest="dummymodel", default="parsets/dummy.model",help="Specify dummy model for use in applying gains [default: %default]")
group.add_option("-z", "--phaseparset", action="store", type="string", dest="phaseparset", default="parsets/phaseonly.parset",help="Specify bbs parset to use on phase only calibration of target [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Data Selection Options:")
group.add_option("-B", "--bandsno", action="store", type="int", dest="bandsno", default=4,help="Specify how many bands there are. [default: %default]")
group.add_option("-S", "--subsinbands", action="store", type="int", dest="subsinbands", default=17,help="Specify how sub bands are in a band. [default: %default]")
group.add_option("-d", "--data", action="store", type="string", dest="data", default="/media/RAIDD/lofar_data/",help="Specify name of the directoy where the data is held (in obs subdirectories) [default: %default]")
group.add_option("-b", "--beams", action="store", type="string", dest="beams", default="0,1,2,3,4,5", help="Use this option to select which beams to process in the format of a list of beams with no spaces \
separated by commas eg. 0,1,2 [default: %default]")
parser.add_option_group(group)
(options, args) = parser.parse_args()


#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Assignment to variables
#----------------------------------------------------------------------------------------------------------------------------------------------

allowedlevels=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
loglevel=options.loglevel.upper()
if loglevel not in allowedlevels:
	print "Logging level {0} not recongised\n\
Must be one of {1}".format(loglevel, allowedlevels)
	sys.exit()

numeric_level = getattr(logging, loglevel, None)

#Setup logging
log=logging.getLogger("rsm")
log.setLevel(logging.DEBUG)
logformat=logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
term=logging.StreamHandler()
term.setLevel(numeric_level)
term.setFormatter(logformat)
log.addHandler(term)

textlog=logging.FileHandler('rsmpp.log', mode='w')
textlog.setLevel(logging.DEBUG)
textlog.setFormatter(logformat)
log.addHandler(textlog)

log.info("Run started at {0} UTC".format(date_time_start))
log.info("rsmpp.py Version {0}".format(vers))

#Set options to variables just to make life a bit easier
autoflag=options.autoflag
image_meth=options.image
initialiters=options.initialiter
maxb=options.maxbaseline
no_mask=options.nomask
mosaic=options.mosaic
ndppp_parset=options.ndppp
mvdir=options.move
n=options.nobservations
newdirname=options.newdir
peeling=options.peeling
shortpeel=options.peelingshort
postcut=options.postcut
overwrite=options.overwrite
toflag=options.flaguser
calparset=options.calparset
data_dir=options.data
calmodel=options.calmodel
correctparset=options.corparset
target_oddeven=options.target_oddeven.lower()
phaseparset=options.phaseparset
dummy=options.dummymodel
skymodel=options.skymodel
root_dir=os.getcwd()
destroy=options.destroy
bandsno=options.bandsno
subsinbands=options.subsinbands
toprocess=options.obsids
if toprocess!="to_process.py":
	if "," in toprocess:
		toprocess_list=sorted(toprocess.split(","))
	elif "-" in toprocess:
		tempsplit=sorted(toprocess.split("-"))
		toprocess_list=["L{0}".format(i) for i in range(int(tempsplit[0][-5:]),int(tempsplit[1][-5:])+1)]
#get the beams
beams=[int(i) for i in options.beams.split(',')]

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Checks & True False Definitions
#----------------------------------------------------------------------------------------------------------------------------------------------
#Below are all just various file and setting checks etc to make sure everything is present and correct or within limits. Some options are not simply
#True or False so flags also have to be set for such options.

log.info("Performing initial checks and assigning settings...")
#Checks the actual parset folder
if os.path.isdir(os.path.join(root_dir, "parsets")) == False:
	log.critical("Parsets directory cannot be found.\n\
Please make sure all parsets are located in a directory named 'parsets'.\n\
Script now exiting...")
	sys.exit()

#Checks number of threads is reasonable
if 0 > n or n > multiprocessing.cpu_count():
	log.critical("Number of cores must be between 1 - {0}\n\
Script now exiting...".format(multiprocessing.cpu_count()))
	sys.exit()

#Whether flagging is to be used
if toflag !="" or autoflag ==True:	#If user selects stations to flag then flag also needs to be set to true.
	flag=True
else:
	flag=False

#Imaging Check inc parsets are present
if image_meth != "None":
	image=True
	if image_meth == "AW" or image_meth=="BOTH":
		if os.path.isfile("parsets/aw.parset") == False:
			log.critical("Cannot find imaging parset file 'aw.parset' in the 'parsets' directory, please check it is present\n\
	Script now exiting...")
			sys.exit()
	elif image_meth == "CASA" or image_meth=="BOTH":
		if os.path.isfile("parsets/casa.parset") == False:
			log.critical("Cannot find imaging parset file 'casa.parset' in the 'parsets' directory, please check it is present\n\
	Script now exiting...")
			sys.exit()
else:
	image=False

#Checks if peeling is to be performed
if peeling!=0:
	topeel=True
else:
	topeel=False

#Checks if post bbs is to be used.
if postcut !=0:
	postbbs=True
else:
	postbbs=False

#Checks directory to move result to
if mvdir != "!":
	move=True
	if os.path.isdir(mvdir) == False:
		log.critical("Directory \"{0}\" to move output to doesn't seem to exist..., please check and mkdir if necessary.\n\
Script now exiting...".format(mvdir))
		sys.exit()
else:
	move=False

#Check presence of to_process.py if needed
if toprocess=="to_process.py":
	if os.path.isfile(toprocess)==False:
		log.critical("Cannot locate 'to_process.py', please check file is present\nScript now exiting...")
		sys.exit()

#Check skymodel creation choice or file
if skymodel=="AUTO":
	create_sky=True
else:
	if not os.path.isfile(skymodel):
		log.error("Cannot locate {0}, please check your skymodel file is present\n\
If you would like to automatically generate a skymodel file do not use the -s option.\nScript now exiting...".format(skymodel))
		sys.exit()
	else:
		create_sky=False
		
if calmodel=="AUTO":
	create_cal=True
else:
	if not os.path.isfile(calmodel):
		log.error("Cannot locate {0}, please check your calmodel file is present\n\
If you would like to automatically fetch the calibrator skymodel do not use the -s option.\nScript now exiting...".format(skymodel))
		sys.exit()
	else:
		create_cal=False	

log.info("Checking required parsets...")
#NDPPP parset
if os.path.isfile(ndppp_parset)==False:
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(ndppp_parset))
	sys.exit()
#Check data dir
if os.path.isdir(data_dir) == False:
	log.critical("Data Directory \"{0}\" doesn't seem to exist..., please check it has been set correctly.\n\
Script now exiting...".format(data_dir))
	sys.exit()
#Check the phase only parset
if os.path.isfile(phaseparset)==False:
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(phaseparset))
	sys.exit()
#Checks presence of the four parset files
if os.path.isfile(calparset)==False:
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(calparset))
	sys.exit()
if os.path.isfile(correctparset)==False:
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(correctparset))
	sys.exit()
if os.path.isfile(dummy)==False:
	log.critical("Cannot locate {0}, please check file is present\nScript now exiting...".format(dummy))
	sys.exit()
	
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Other Pre-Run Checks & Directory Change
#----------------------------------------------------------------------------------------------------------------------------------------------

# Checks that the output directory is not already present, overwrites if -w is used
if os.path.isdir(newdirname) == True:
	if overwrite==True:
		log.info("Removing previous results directory...")
		subprocess.call("rm -rf {0}".format(newdirname), shell=True)
	else:
		log.critical("Directory \"{0}\" already exists and overwrite option not used, run again with '-w' option to overwrite directory or rename/move old results file\n\
Script now exiting...".format(newdirname))
		sys.exit()
		
# Makes the new directory and moves to it
os.mkdir(newdirname)
os.chdir(newdirname)
working_dir=os.getcwd()

# Copies over all relevant files needed
subprocess.call(["cp","-r","../parsets","../to_process.py", "."])
if not os.path.isdir('logs'):
	os.mkdir('logs')
if not os.path.isdir('plots'):
	os.mkdir('plots')

#Checks for monitoring and starts if chosen
if options.monitor==True:
	log.info("System Monitoring Starting...")
	if __name__ == '__main__':
		par,child = multiprocessing.Pipe()
		p=rsmf.MemoryMonitor(user, pipe = child)
		p.start()

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Load in User List and Check Data Presence
#----------------------------------------------------------------------------------------------------------------------------------------------

#Gets the to_process list and assigns it to target_obs
log.info("Collecting observations to process...")

if toprocess=="to_process.py":
	from to_process import to_process
	target_obs=to_process
	target_obs.sort()
else:
	target_obs=toprocess_list

#This splits up the sets to process into targets and calibrators.
odd=[]
even=[]
firstid=target_obs[0]
if int(firstid[-5:])%2 == 0:
	firstid_oe="even"
else:
	firstid_oe="odd"

for obs in target_obs:
	if int(obs[-5:])%2 == 0:
		even.append(obs)
	else:
		odd.append(obs)
		
if target_oddeven=="even":
	target_obs=even
	calib_obs=odd
else:
	target_obs=odd
	calib_obs=even

#The following passage just checks that all the data is present where it should be.
log.info("Observations to be processed:")
for i in target_obs:
	log.info(i)
	if os.path.isdir(os.path.join(data_dir,i))==False:
		log.critical("Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir))
		sys.exit()
	if not os.path.isdir(i):
		subprocess.call("mkdir -p {0}/plots {0}/logs {0}/flagging {0}/datasets".format(i), shell=True)
log.info("Calibrators to be processed:")
for i in calib_obs:
	log.info(i)
	if os.path.isdir(os.path.join(data_dir,i))==False:
		log.critical("Calibrator Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir))
		sys.exit()
	if not os.path.isdir(i):
		subprocess.call("mkdir -p {0}/plots {0}/logs".format(i), shell=True)


#----------------------------------------------------------------------------------------------------------------------------------------------
#																Search for Missing Sub bands
#----------------------------------------------------------------------------------------------------------------------------------------------

#Get ready for reporting any missing sub bands.
missing_count=0
g=open("missing_subbands.txt", 'w')

#RSM searching, has to check for all snapshots, and calibrators next block of code checks for missing sub bands and organises bands
targets=rsmf.Ddict(dict)	#Dictionary of dictionaries to store target observations
calibs={}
missing_calibrators={}
missing_calib_count=0
rsm_bands={}	#Store the measurement sets in terms of bands
rsm_bands_lens={}	#Store the length of each band (needed as some might be missing)
diff=bandsno*subsinbands
# diff=34
rsm_band_numbers=range(bandsno)
# rsm_band_numbers=range(3)

log.info("Collecting and checking sub bands of observations..")
for i,j in izip(target_obs, calib_obs):
	missing_calibrators[i]=[]
	calibglob=os.path.join(data_dir,j,'*.MS.dppp')
	calibs[j]=sorted(glob.glob(calibglob))
	log.debug(calibs[j])
	if len(calibs[j])<1:
		log.critical("Cannot find any measurement sets in directory {0} !".format(os.path.join(data_dir,j)))
		sys.exit()
	calibs_first=0		#Should always start on 0
	calibs_last=int(calibs[j][-1].split('SB')[1][:3])	#Last one present
	calib_range=range(calibs_first, calibs_last+1)		#Range of present (if last one is missing then this will be realised when looking at targets) 
	present_calibs=[]
	for c in calibs[j]:
		SB=int(c.split('SB')[1][:3])				#Makes a list of all the Calib sub bands present
		present_calibs.append(SB)
	for s in calib_range:
		if s not in present_calibs:
			missing_calibrators[i].append(s)		#Checks which ones are missing and records them
			g.write("SB{0} calibrator missing in observation {1}\n".format('%03d' % s, j))
			missing_count+=1
	for b in beams:
		#This now uses a function to check all the targets, now knowing what calibs are missing - which without nothing can be done
		rsmf.check_targets(i, b, targets, rsm_bands, rsm_band_numbers, rsm_bands_lens, missing_calibrators, data_dir, diff, g, subsinbands)

g.close()

#Give feedback as to whether any are missing or not.
if missing_count>0:
	log.warning("Some sub bands appear to be missing - see generated file 'missing_subbands.txt' for details")
else:
	#Just remove the file if none are missing
	os.remove("missing_subbands.txt")	

log.debug("Check this one")

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Main Run
#----------------------------------------------------------------------------------------------------------------------------------------------
#Create multiprocessing Pool
worker_pool = Pool(processes=n)

# #Reads in NDPPP parset file ready for use
n_temp=open(ndppp_parset, 'r')
ndppp_base=n_temp.readlines()
n_temp.close()
# 
# Following is the main run -> create models -> Initial NDPPP -> Calibrate -> (Peeling) -> (Post bbs) -> concatenate
# 
#Detect which calibrator (CURRENTLY DEPENDS ALOT ON THE MS TABLE BEING CONSISTENT and CALIBRATOR BEING SAME THROUGHOUT WHOLE OBS and NAMED AS JUST CALIBRATOR)
#Needs a fail safe of coordinates detection
if create_cal:
	log.info("Detecting calibrator and obtaining skymodel...")
	calib_ms=pt.table(calibs[calibs.keys()[0]][0]+"/OBSERVATION")
	calib_name=calib_ms.col("LOFAR_TARGET")[0][0].replace(" ", "")
	log.info("Calibrator Detected: {0}".format(calib_name))
	calmodel="{0}.skymodel".format(calib_name)
	if not os.path.isfile(os.path.join("/home/as24v07/skymodels", calmodel)):
		log.critical("Could not find calibrator skymodel...")
		if mail==True:
			em.send_email(emacc,user_address,"slofarpp Job Error","{0},\n\nYour job {1} has encountered an error - calibrator skymodel could not be found for Calibrator {2}".format(user,newdirname, calib_name))
		sys.exit()
	subprocess.call(["cp", "/home/as24v07/skymodels/{0}".format(calmodel), "parsets/"])
	calmodel=os.path.join("parsets", calmodel)
	calib_ms.close()
		
# Builds parmdb files as these are used over and over
log.info("Building calibrator sourcedb...")
if os.path.isdir('sky.calibrator'):
	subprocess.call("rm -rf sky.calibrator", shell=True)
subprocess.call("makesourcedb in={0} out=sky.calibrator format=\'<\' > logs/skysourcedb.log 2>&1".format(calmodel), shell=True)

log.info("Building dummy sourcedb...")
if os.path.isdir('sky.dummy'):
	subprocess.call("rm -rf sky.dummy", shell=True)
subprocess.call("makesourcedb in={0} out=sky.dummy format=\'<\'  > logs/dummysourcedb.log 2>&1".format(dummy), shell=True)

#Creates the sky model for each pointing using script that creates sky model from measurement set.
if create_sky:
	log.info("Creating skymodels for each beam...")
	for b in beams:
		beamc="SAP00{0}".format(b)
		skymodel="parsets/{0}.skymodel".format(beamc)
		subprocess.call("/home/as24v07/scripts/gsm_ms.py -r {0} -c 0.1 {1} {2} > logs/{3}_skymodel_log.txt 2>&1".format(options.skyradius, targets[targets.keys()[0]][beamc][0], skymodel, beamc), shell=True)
		if image:
			subprocess.call("makesourcedb in={0} out={1} format=\'<\' > logs/skysourcedb_{2}.log 2>&1".format(skymodel, skymodel.replace(".skymodel", ".sky"), beamc), shell=True)

#Now working through the steps starting with NDPPP (see rsmpp_funcs.py for functions)
for i,j in izip(sorted(targets.keys()), sorted(calibs)):
	log.info("Starting Initial NDPPP for {0} and {1}...".format(i, j))
	current_obs=i
	#Nearly all functions are used with partial such that they can be passed to .map
	NDPPP_Initial_Multi=partial(rsmf.NDPPP_Initial, wk_dir=working_dir, ndppp_base=ndppp_base)
	if __name__ == '__main__':
		worker_pool.map(NDPPP_Initial_Multi, calibs[j])
		calibs[j] = sorted(glob.glob(os.path.join(j,"*.tmp")))
	for b in beams:
		beam=b
		beamc="SAP00{0}".format(b)
		if __name__ == '__main__':
			worker_pool.map(NDPPP_Initial_Multi, targets[i][beamc])
		targets[i][beamc] = sorted(glob.glob(i+"/*.tmp"))

	
	log.info("Done!")
	# calibrate step 1 process
	log.info("Calibrating calibrators and transfering solutions for {0} and {1}...".format(i, j))
	calibrate_msss1_multi=partial(rsmf.calibrate_msss1, beams=beams, diff=diff, calparset=calparset, calmodel=calmodel, correctparset=correctparset, dummy=dummy, oddeven=target_oddeven, firstid=firstid_oe)
	if __name__ == '__main__':
		worker_pool.map(calibrate_msss1_multi, calibs[j])

log.info("Done!")
#Combine the bands
log.info("Creating Bands for all sets...")
rsm_bandsndppp_multi=partial(rsmf.rsm_bandsndppp, rsm_bands=rsm_bands)
if __name__ == '__main__':
	worker_pool.map(rsm_bandsndppp_multi, rsm_bands.keys())
log.info("Done!")

log.info("Performing phaseonly calibration (and flagging if selected) on all sets...")
# calibrate step 2 process
tocalibrate=sorted(glob.glob("L*/L*_SAP00?_BAND??.MS.dppp.tmp"))
calibrate_msss2_multi=partial(rsmf.calibrate_msss2, phaseparset=phaseparset, flag=flag, toflag=toflag, autoflag=autoflag, create_sky=create_sky, skymodel=skymodel)
if __name__ == '__main__':
	worker_pool.map(calibrate_msss2_multi, tocalibrate)
proc_target_obs=sorted(glob.glob("L*/L*_SAP00?_BAND??.MS.dppp"))
log.info("Done!")

if topeel:
	log.info("Peeling process started on all sets...")
	peeling_steps_multi=partial(rsmf.peeling_steps, shortpeel=shortpeel, peelsources=peelsources)
	os.mkdir("prepeeled_sets")
	if __name__=='__main__':
		# pool_peeling=mpl(processes=n)
		worker_pool.map(peeling_steps_multi, proc_target_obs)
	print "Done!"

if postbbs==True:
	log.info("Post-bbs clipping process started on all sets...")
	post_bbs_multi=partial(rsmf.post_bbs, postcut=postcut)
	if __name__ == '__main__':
		# pool_postbbs = Pool(processes=n)
		worker_pool.map(post_bbs_multi, proc_target_obs)
	log.info("Done!")

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Final Concat Step for MSSS style
#----------------------------------------------------------------------------------------------------------------------------------------------

# for be in beams:
# 	print "Final conatenate process started..."
# 	# snapshot_concat_multi=partial(rsmf.snapshot_concat, beam=be)	#Currently cannot combine all bands in a snapshot (different number of subands)
# 	final_concat_multi=partial(rsmf.final_concat, beam=be, target_obs=target_obs, rsm_bands_lens=rsm_bands_lens)
# 	if __name__ == '__main__':
# 		pool_concat = Pool(processes=5)
# 		# pool_snapconcat = Pool(processes=5)
# 		# pool_snapconcat.map(snapshot_concat_multi, sorted(target_obs))
# 		pool_concat.map(final_concat_multi, rsm_band_numbers)
# 	print "Done!"

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Imaging Step
#----------------------------------------------------------------------------------------------------------------------------------------------

#Loops through each group folder and launches an AWimager step or CASA
if image==True:
	#Switch to new awimager
	awimager_environ=rsmf.convert_newawimager(os.environ.copy())
			
	# globterms=["L*/L*BAND*.MS.dppp", "SAP00*BAND*_FINAL.MS"]		
	toimage=sorted(glob.glob("L*/L*BAND*.MS.dppp"))
	
	if image_meth=="AW" or image_meth=="BOTH":
		log.info("Starting imaging process with AWimager...")
		#Need to create sky model data
		image_file=open("parsets/aw.parset", 'r')
		aw_sets=image_file.readlines()
		image_file.close()
		mask_size=""
		to_remove=[]
		for s in aw_sets:
			if "npix=" in s or "cellsize=" in s or "data=" in s:
				mask_size+=" "+s.strip('\n')
			if "niter=" in s:
				niters=int(s.split("=")[1])
				to_remove.append(s)
			if "threshold=" in s:
				to_remove.append(s)
		for j in to_remove:
			aw_sets.remove(j)
		log.info("Maximum baseline to image: {0}m".format(maxb))
		if not no_mask:
			create_mask_multi=partial(rsmf.create_mask, mask_size=mask_size, toimage=toimage)
			if __name__ == '__main__':
				worker_pool.map(create_mask_multi,beams)
		AW_Steps_multi=partial(rsmf.AW_Steps, aw_sets=aw_sets, maxb=maxb, aw_env=awimager_environ, niter=niters, initialiter=initialiters, nomask=no_mask)
		if __name__ == '__main__':
			pool = Pool(processes=1)
			pool.map(AW_Steps_multi,toimage)
		log.info("Done!")
		
		
	if image_meth=="CASA" or image_meth=="BOTH":
		log.info("Starting imaging process with Casa...")
		settings=open('parsets/casa.parset', 'r')
		casa_sets=settings.readlines()
		settings.close()
		CASA_STEPS_multi=partial(rsmf.CASA_STEPS, casa_sets=casa_sets)
		if __name__ == '__main__':
			pool = Pool(processes=2)
			result = pool.map(CASA_STEPS_multi,toimage)
		log.info("Done!")

	log.info("Tidying up imaging...")
	for i in target_obs:
		os.chdir(i)
		os.mkdir("images")
		subprocess.call("mv *.fits images", shell=True)
		if image_meth=="CASA" or image_meth=="BOTH":
			subprocess.call("mv *.image *.flux *.model *.residual *.psf images", shell=True)
		if image_meth=="AW" or image_meth=="BOTH":
			subprocess.call("mv *.model *.residual *.psf *.restored *.img0.avgpb *.img0.spheroid_cut* *.corr images", shell=True)
		os.chdir("..")
	log.info("Creating averaged images...")
	average_band_images_multi=partial(rsmf.average_band_images, beams=beams)
	if __name__=='__main__':
		worker_pool.map(average_band_images_multi, target_obs)
	if mosaic:
		create_mosaic_multi=partial(rsmf.create_mosaic, band_nums=rsm_band_numbers)
		for i in target_obs:
			pool=Pool(processes=len(rsm_band_numbers))
			pool.map(create_mosaic_multi, target_obs)
	
#----------------------------------------------------------------------------------------------------------------------------------------------
#																End of Process
#----------------------------------------------------------------------------------------------------------------------------------------------
 
#Finishes up and moves the directory if chosen, performing checks
log.info("Tidying up...")
# os.mkdir("FINAL_COMB_DATASETS")
# subprocess.call("mv SAP00*BAND*_FINAL.MS FINAL_COMB_DATASETS", shell=True)
for c in calib_obs:
	subprocess.call("mkdir {0}/datasets {0}/parmdb_tables".format(c), shell=True)
	subprocess.call("mv {0}/*.pdf {0}/plots".format(c), shell=True)
	subprocess.call("mv {0}/*.tmp {0}/datasets".format(c), shell=True)
	subprocess.call("mv {0}/*.parmdb {0}/parmdb_tables".format(c), shell=True)
os.mkdir("Calibrators")
mv_calibs=["mv",]+sorted(calib_obs)
mv_calibs.append("Calibrators")
subprocess.call(mv_calibs)
for t in target_obs:
	subprocess.call("mv {0}/*_uv.MS.dppp {0}/datasets".format(t), shell=True)
	subprocess.call("mv {0}/*.stats {0}/*.pdf {0}/*.tab {0}/flagging".format(t), shell=True)
subprocess.call(["rm","-r","sky.calibrator","sky.dummy"])

if destroy:
	log.warning("Destroy Mode Selected, now deleting mostly everything...")
	for c in calib_obs:
		subprocess.call(["rm", "-r", "Calibrators/{0}/parmdb_tables".format(c)])
	for t in target_obs:
		subprocess.call("rm -r {0}/datasets".format(t), shell=True)

log.info("All processed successfully!")
if move==True:
	subprocess.call("cp -r {0} {1}{0}".format(newdirname, mvdir), shell=True)
	if os.path.isdir("{0}{1}".format(mvdir, newdirname))==True:
		subprocess.call("rm -rf {0}".format(newdirname), shell=True)
		log.info("Results can be found in {0} ".format("{0}{1}".format(mvdir, newdirname), shell=True))
	else:
		log.warning("Could not confirm that results were moved sucessfully, results have not been deleted from current directory")
else:
	log.info("Results can be found in {0}".format(newdirname))
	
end=datetime.datetime.utcnow()
date_time_end=end.strftime("%d-%b-%Y %H:%M:%S")
tdelta=end-now

if options.monitor==True:
	if __name__ == '__main__':
		p.shutdown()
		statslog=par.recv()
		p.join()
		syslog=open("system_log.txt", "w")
		syslog.write("Time Started: {0} by User: {1}\n".format(date_time_start, user))
		syslog.write("Time\t\tMemory\t\tCPU%\n")
		syslog.write("----------------------------------------------------------------\n")
		for i in statslog:
			syslog.write(i+"\n")
	syslog.write("----------------------------------------------------------------\n")
	syslog.write("\nTime Finished: {0} taking: {1}\n".format(date_time_end, tdelta))
	syslog.close()
if mail==True:
	if user=="mp":
		em.send_email(emacc,user_address,"slofarpp Job Completed","Gogo!,\n\nYour job {1} has been completed - finished at {2} UTC with a runtime of {3}\n\nDobry! Ze nie zlamac... byc moze.\n\nWhaaaatt? Jak moge powiedziec it properly...".format(user,newdirname, date_time_end, tdelta))
	else:
		em.send_email(emacc,user_address,"slofarpp Job Completed","{0},\n\nYour job {1} has been completed - finished at {2} UTC with a runtime of {3}".format(user,newdirname, date_time_end, tdelta))

os.chdir("..")
subprocess.call("rm emailslofar.py* quick_keys.py*", shell=True)
log.info("Run finished at {0} UTC with a runtime of {1}".format(date_time_end, str(tdelta)))
subprocess.call(["cp", "rsmpp.log", "{0}/rsmpp_{0}.log".format(newdirname)])