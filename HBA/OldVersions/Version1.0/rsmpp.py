#!/usr/bin/env python

#rsmpp.py

#Run 'python rsmpp.py -h' for full details and usage.

#LOFAR data processing script designed for the Southampton lofar machines.

#A full user guide can be found on google docs here:
#https://docs.google.com/document/d/1p4iGIKRjt9feb4vJXYcLO_h4HB_lQzQ5SpRxVG3UKTk/edit

#Written by Adam Stewart, July 2012, Last Update January 2013

#---Version 1.0---
#All use automatic 

import subprocess, multiprocessing, os, glob, optparse, sys, datetime, string, getpass, re, time
from functools import partial
from multiprocessing import Pool
import pyrap.tables as pt
from itertools import izip
subprocess.call(["cp", "/home/as24v07/scripts/emailslofar.py", "/home/as24v07/scripts/quick_keys.py", "."])
import emailslofar as em
import rsmpp_funcs as rsmf
vers="1.0"
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
newdirname="slofarpp_{0}".format(now.strftime("%H:%M:%S_%d-%b-%Y"))
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Optparse and linking to parameters + checks
#----------------------------------------------------------------------------------------------------------------------------------------------
usage = "usage: python %prog [options]"
description="This script has been written to unify the current techniques used to process and image LOFAR data. There are four modes to choose from: standard imaging pipeline (SIP), HBA MSSS style 1 (HBAMSSS1), \
HBA MSSS style 2 (HBAMSSS2) and LBA MSSS (LBAMSSS). All parsets should be placed in a parsets directory in the working area, and the to_process.py script is required which specifies the list of observations or \
snapshots to process.\n\
For full details on how to run the script, see the user manual here: https://docs.google.com/document/d/1p4iGIKRjt9feb4vJXYcLO_h4HB_lQzQ5SpRxVG3UKTk/edit\n\
By no means is this a 'tidy' script, but has been over written to an extent in an attempt to make each process and mode easy to follow."
parser = optparse.OptionParser(usage=usage,version="%prog v{0}".format(vers), description=description)

group = optparse.OptionGroup(parser, "General Options")
group.add_option("--monitor", action="store_true", dest="monitor", default=False, help="Turn on system monitoring [default: %default]")
group.add_option("-f", "--flag", action="store_true", dest="autoflag", default=False,help="Use this option to use autoflagging in processing [default: %default]")
group.add_option("-i", "--image", action="store",type="string", dest="image", default="None",help="Use this option to image the results with AWimager - 'AW' or Casa - 'CASA', enter settings in a textfile \
named 'awimager.parset' ms=GROUP.MS and imagename=IMAGE or 'casa.parset with just a list of image settings (do not include vis or imagename), or of course you could enter 'BOTH'... [default: %default]")
group.add_option("-m", "--move", action="store", type="string", dest="move", default="!",help="Specify the path of where output should be moved to [default: {0}]".format(os.getcwd()+"/"))
group.add_option("-n", "--nobservations", action="store", type="int", dest="nobservations", default=4, help="Specify the number of observations to process simultaneously (i.e. the number of threads)[default: %default]")
group.add_option("-j", "--target_oddeven", action="store", type="string", dest="target_oddeven", default="odd",help="Specify whether the targets are the odd numbered observations or the even [default: %default]")
group.add_option("-o", "--output", action="store", type="string", dest="newdir", default=newdirname,help="Specify name of the directoy that the output will be stored in [default: %default]")
group.add_option("-p", "--peeling", action="store", type="int", dest="peeling", default=0,help="Use this option to enable peeling, specifying how many sources to peel [default: %default]")
group.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False,help="Use this option to run in quiet mode [default: %default]")
group.add_option("-v", "--peelingshort", action="store_true", dest="peelingshort", default=False,help="Use this option to skip the last section of the peeling procedure and NOT add back in the peeled sources [default: %default]")
group.add_option("-c", "--peelsources", action="store", type="string", dest="peelsources", default="0",help="Use this option to specify which sources to peel instead of the code taking the X brightest sources. Enter in the format\
 source1,source2,source3,.... [default: None]")
group.add_option("-t", "--postcut", action="store", type="int", dest="postcut", default=0,help="Use this option to enable post-bbs flagging, specifying the cut level [default: %default]")
group.add_option("-u", "--flaguser", action="store", type="string", dest="flaguser", default="",help="Give specific stations to flag out, enter in the format of: !CS001LBA;!CS002LBA; (MUST end with ;) [default: None]")
group.add_option("-w", "--overwrite", action="store_true", dest="overwrite", default=False,help="Use this option to overwrite output directory if it already exists [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Parset Options:")
group.add_option("-a", "--calparset", action="store", type="string", dest="calparset", default="parsets/cal.parset",help="Specify bbs parset to use on calibrator calibration [default: %default]")
group.add_option("-e", "--calmodel", action="store", type="string", dest="calmodel", default="parsets/cal.skymodel",help="Specify the calibrator skymodel [default: %default]")
group.add_option("-g", "--corparset", action="store", type="string", dest="corparset", default="parsets/correct.parset",help="Specify bbs parset to use on gain transfer to target [default: %default]")
group.add_option("-k", "--ndppp", action="store", type="string", dest="ndppp", default="parsets/ndppp.1.initial.parset",help="Specify the template initial NDPPP file to use [default: %default]")
group.add_option("-s", "--skymodel", action="store", type="string", dest="skymodel", default="AUTO",help="Specify a particular field skymodel to use for the phase only calibration, by default the skymodels will be\
automatically generated.[default: %default]")
group.add_option("-y", "--dummymodel", action="store", type="string", dest="dummymodel", default="parsets/dummy.model",help="Specify dummy model for use in applying gains [default: %default]")
group.add_option("-z", "--phaseparset", action="store", type="string", dest="phaseparset", default="parsets/phaseonly.parset",help="Specify bbs parset to use on phase only calibration of target [default: %default]")
parser.add_option_group(group)
group = optparse.OptionGroup(parser, "Data Selection Options:")
group.add_option("-d", "--data", action="store", type="string", dest="data", default="/media/RAIDA/lofar_data/",help="Specify name of the directoy where the data is held (in obs subdirectories) [default: %default]")
group.add_option("-l", "--beams", action="store", type="string", dest="beams", default="0", help="Use this option to select which beam of the MSSS data to use. For LBAMSSS or HBAMSSS1 it needs to be one digit. \
For HBAMSSS2 and RSM this can be a list of beams with no spaces separated by commas eg. 0,1,2 [default: %default]")
parser.add_option_group(group)
(options, args) = parser.parse_args()


#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Assignment to variables
#----------------------------------------------------------------------------------------------------------------------------------------------
print "Run started at {0} UTC".format(date_time_start)
print "rsmpp.py Version {0}".format(vers)

#Set options to variables
autoflag=options.autoflag
image_meth=options.image
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
qu=options.quiet
root_dir=os.getcwd()

beams=[int(i) for i in options.beams.split(',')]

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Options Checks & True False Definitions
#----------------------------------------------------------------------------------------------------------------------------------------------
print "Performing initial checks and assigning settings..."
#Checks the actual parset folder
if os.path.isdir(os.path.join(root_dir, "parsets")) == False:
	print "Parsets directory cannot be found.\n\
Please make sure all parsets are located in a directory named 'parsets'.\n\
Script now exiting..."
	sys.exit()

#nobservations
if 0 > n or n > multiprocessing.cpu_count():
	print "Number of cores must be between 1 - {0}\n\
Script now exiting...".format(multiprocessing.cpu_count())
	sys.exit()

#Flagging
if toflag !="" or autoflag ==True:	#If user selects stations to flag then flag also needs to be set to true.
	flag=True
else:
	flag=False

#Imaging Check
if image_meth != "None":
	image=True
	if image_meth == "AW" or image_meth=="BOTH":
		if os.path.isfile("parsets/aw.parset") == False:
			print "Cannot find imaging parset file 'aw.parset' in the 'parsets' directory, please check it is present\n\
	Script now exiting..."
			sys.exit()
	elif image_meth == "CASA" or image_meth=="BOTH":
		if os.path.isfile("parsets/casa.parset") == False:
			print "Cannot find imaging parset file 'casa.parset' in the 'parsets' directory, please check it is present\n\
	Script now exiting..."
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
		print "Directory \"{0}\" to move output to doesn't seem to exist..., please check and mkdir if necessary.\n\
Script now exiting...".format(mvdir)
		sys.exit()
else:
	move=False


#Checks presence of important parsets/files

if os.path.isfile("to_process.py")==False:
	print "Cannot locate 'to_process.py', please check file is present"
	print "Script now exiting..."
	sys.exit()


if skymodel=="AUTO":
	create_sky=True
else:
	if not os.path.isfile(skymodel):
		print "Cannot locate {0}, please check your skymodel file is present\n\
	If you would like to automatically generate a skymodel file do not use the -s option.".format(skymodel)
		print "Script now exiting..."
		sys.exit()
	else:
		create_sky=False	

if os.path.isfile(ndppp_parset)==False:
	print "Cannot locate {0}, please check file is present".format(ndppp_parset)
	print "Script now exiting..."
	sys.exit()

#Now checks mode specific parsets


print "Checking required parsets..."
#Check data dir
if os.path.isdir(data_dir) == False:
	print "Data Directory \"{0}\" doesn't seem to exist..., please check it has been set correctly.\n\
Script now exiting...".format(mvdir)
	sys.exit()
#Check the phase only parset
if os.path.isfile(phaseparset)==False:
	print "Cannot locate {0}, please check file is present".format(phaseparset)
	print "Script now exiting..."
	sys.exit()
	#Checks presence of the four parset files
if os.path.isfile(calparset)==False:
	print "Cannot locate {0}, please check file is present".format(calparset)
	print "Script now exiting..."
	sys.exit()
if os.path.isfile(correctparset)==False:
	print "Cannot locate {0}, please check file is present".format(correctparset)
	print "Script now exiting..."
	sys.exit()
if os.path.isfile(dummy)==False:
	print "Cannot locate {0}, please check file is present".format(dummy)
	print "Script now exiting..."
	sys.exit()
#THIS NEEDS TO BECOME AUTOMATIC
# if os.path.isfile(calmodel)==False:
# 	print "Cannot locate {0}, please check file is present".format(calmodel)
# 	print "Script now exiting..."
# 	sys.exit()
	
#----------------------------------------------------------------------------------------------------------------------------------------------
#																Other Pre-Run Checks & Directory Change
#----------------------------------------------------------------------------------------------------------------------------------------------

# Checks that the output directory is not already present, overwrites if -w is used
if os.path.isdir(newdirname) == True:
	if overwrite==True:
		print "Removing previous results directory..."
		subprocess.call("rm -rf {0}".format(newdirname), shell=True)
	else:
		print "Directory \"{0}\" already exists and overwrite option not used, run again with '-w' option to overwrite directory or rename/move old results file\n\
Script now exiting...".format(newdirname)
		sys.exit()
		
# Makes the new directory
os.mkdir(newdirname)
os.chdir(newdirname)
working_dir=os.getcwd()

# Copies over all relevant files
subprocess.call(["cp","-r","../parsets","../to_process.py", "."])
if not os.path.isdir('logs'):
	os.mkdir('logs')
if not os.path.isdir('plots'):
	os.mkdir('plots')
	
if options.monitor==True:
	print "System Monitoring Starting..."
	if __name__ == '__main__':
		par,child = multiprocessing.Pipe()
		p=rsmf.MemoryMonitor('as24v07', pipe = child)
		p.start()

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Load in User List and Check Data Presence
#----------------------------------------------------------------------------------------------------------------------------------------------

#Gets the to_process list and assigns it to target_obs
print "Collecting observations to process..."
from to_process import to_process
target_obs=to_process

#This splits up the sets to process into targets and calibrators.
odd=[]
even=[]
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

#The following passage just checks that all the data is present.
print "Observations to be processed:"
for i in target_obs:
	print i
	if os.path.isdir(os.path.join(data_dir,i))==False:
		print "Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir)
		sys.exit()
	if not os.path.isdir(i):
		subprocess.call("mkdir -p {0}/plots {0}/logs {0}/flagging {0}/datasets".format(i), shell=True)
print "Calibrators to be processed:"
for i in calib_obs:
	print i
	if os.path.isdir(os.path.join(data_dir,i))==False:
		print "Calibrator Snapshot {0} cannot be located in data directory {1}, please check.\n\
Script now exiting...".format(i, data_dir)
		sys.exit()
	if not os.path.isdir(i):
		subprocess.call("mkdir -p {0}/plots {0}/logs".format(i), shell=True)


#----------------------------------------------------------------------------------------------------------------------------------------------
#																Search for Missing Sub bands
#----------------------------------------------------------------------------------------------------------------------------------------------

#Get ready for reporting any missing sub bands.
missing_count=0
g=open("missing_subbands.txt", 'w')

#RSM searching, has to check for all snapshots, and calibrators
targets=rsmf.Ddict(dict)
calibs={}
missing_calibrators={}
last_subbands={}
missing_calib_count=0
rsm_bands=rsmf.Ddict(dict)
diff=34
rsm_band_numbers=[0,1,2]

print "Collecting and checking sub bands of observations.."
for i,j in izip(target_obs, calib_obs):
	missing_calibrators[i]=[]
	calibglob=os.path.join(data_dir,j,'*.MS.dppp')
	calibs[j]=sorted(glob.glob(calibglob))
	if len(calibs[j])<1:
		print "Cannot find any measurement sets in directory {0} !".format(os.path.join(data_dir,j))
		sys.exit()
	calibs_first=0
	calibs_last=int(calibs[j][-1].split('SB')[1][:3])
	calib_range=range(calibs_first, calibs_last+1)
	present_calibs=[]
	for c in calibs[j]:
		SB=int(c.split('SB')[1][:3])
		present_calibs.append(SB)
	for s in calib_range:
		if s not in present_calibs:
			missing_calibrators[i].append(s)
			g.write("SB{0} calibrator missing in observation {1}\n".format('%03d' % s, j))
			missing_count+=1
	for b in beams:
		rsmf.check_targets(i, b, targets, last_subbands, rsm_bands, rsm_band_numbers, missing_calibrators, data_dir, diff, g)

g.close()

#Give feedback as to whether any are missing or not.
if missing_count>0:
	print "Some sub bands appear to be missing - see generated file 'missing_subbands.txt' for details"
else:
	#Just remove the file if none are missing
	os.remove("missing_subbands.txt")	

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Main Run
#----------------------------------------------------------------------------------------------------------------------------------------------

worker_pool = Pool(processes=n)

#Reads in NDPPP parset file ready for use
n_temp=open(ndppp_parset, 'r')
ndppp_base=n_temp.readlines()
n_temp.close()
# 
# #The following are the list of commands, if you like, of what to do in each mode. Some commands could be combined, but for readability sake, each
# #mode has been explicitly laid out. Each follow the same basic pattern -> NDPPP -> Calibrate -> Peeling -> Post bbs NDPPP if needed.
# 			
# Builds parmdb files as these are used over and over
print "Building calibrator sourcedb..."
if os.path.isdir('sky.calibrator') == True:
	subprocess.call("rm -rf sky.calibrator", shell=True)
subprocess.call("makesourcedb in={0} out=sky.calibrator format=\'<\' > logs/skysourcedb.log 2>&1".format(calmodel), shell=True)

print "Building dummy sourcedb..."
if os.path.isdir('sky.dummy') == True:
	subprocess.call("rm -rf sky.dummy", shell=True)
subprocess.call("makesourcedb in={0} out=sky.dummy format=\'<\'  > logs/dummysourcedb.log 2>&1".format(dummy), shell=True)

if create_sky==True:
	print "Creating skymodels for each beam..."
	for b in beams:
		beamc="SAP00{0}".format(b)
		skymodel="parsets/{0}.skymodel".format(beamc)
		subprocess.call("/home/as24v07/scripts/gsm_ms.py -r 5 -c 0.1 {0} {1} > logs/{2}_skymodel_log.txt 2>&1".format(targets[targets.keys()[0]][beamc][0], skymodel, beamc), shell=True)
		
for i,j in izip(sorted(targets.keys()), sorted(calibs)):
	print "Starting Initial NDPPP for {0} and {1}...".format(i, j)
	current_obs=i
	NDPPP_Initial_Multi=partial(rsmf.NDPPP_Initial, wk_dir=working_dir, ndppp_base=ndppp_base, quiet=qu)
	if __name__ == '__main__':
		worker_pool.map(NDPPP_Initial_Multi, calibs[j])
		calibs[j] = sorted(glob.glob(os.path.join(j,"*.tmp")))
	for b in beams:
		beam=b
		beamc="SAP00{0}".format(b)
		if __name__ == '__main__':
			worker_pool.map(NDPPP_Initial_Multi, targets[i][beamc])
		targets[i][beamc] = sorted(glob.glob(i+"/*.tmp"))

	
	print "Done!"	# calibrate process
	print "Calibrating calibrators and transfering solutions for {0} and {1}...".format(i, j)
	calibrate_msss1_multi=partial(rsmf.calibrate_msss1, beams=beams, diff=diff, calparset=calparset, calmodel=calmodel, correctparset=correctparset, dummy=dummy, quiet=qu)
	if __name__ == '__main__':
		# pool_bbs1 = Pool(processes=n)
		worker_pool.map(calibrate_msss1_multi, calibs[j])

	print "Done!"	
	print "Creating Bands for {0}...".format(i)
	rsm_bandsndppp_multi=partial(rsmf.rsm_bandsndppp, rsm_bands=rsm_bands, quiet=qu)
	if __name__ == '__main__':
		# pool_ndppp_bands = Pool(processes=n)
		worker_pool.map(rsm_bandsndppp_multi, rsm_bands.keys())
	print "Done!"

print "Performing phaseonly calibration (and flagging if selected) on all sets..."
tocalibrate=sorted(glob.glob("L*/L*_SAP00?_BAND??.MS.dppp.tmp"))
calibrate_msss2_multi=partial(rsmf.calibrate_msss2, phaseparset=phaseparset, flag=flag, toflag=toflag, autoflag=autoflag, create_sky=create_sky, skymodel=skymodel, quiet=qu)
if __name__ == '__main__':
	# pool_bbs = Pool(processes=n)
	worker_pool.map(calibrate_msss2_multi, tocalibrate)
proc_target_obs=sorted(glob.glob("L*/L*_SAP00?_BAND??.MS.dppp"))
print "Done!"		

if topeel:
	print "Peeling process started on all sets..."
	peeling_steps_multi=partial(rsmf.peeling_steps, shortpeel=shortpeel, peelsources=peelsources, quiet=qu)
	os.mkdir("prepeeled_sets")
	if __name__=='__main__':
		# pool_peeling=mpl(processes=n)
		worker_pool.map(peeling_steps_multi, proc_target_obs)
	print "Done!"

if postbbs==True:
	print "Post-bbs clipping process started on all sets..."
	post_bbs_multi=partial(rsmf.post_bbs, postcut=postcut, quiet=qu)
	if __name__ == '__main__':
		# pool_postbbs = Pool(processes=n)
		worker_pool.map(post_bbs_multi, proc_target_obs)
	print "Done!"

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Final Concat Step for MSSS style
#----------------------------------------------------------------------------------------------------------------------------------------------

for be in beams:
	print "Final conatenate process started..."
	# snapshot_concat_multi=partial(rsmf.snapshot_concat, beam=be)
	final_concat_multi=partial(rsmf.final_concat, beam=be, quiet=qu)
	if __name__ == '__main__':
		pool_concat = Pool(processes=5)
		# pool_snapconcat = Pool(processes=5)
		# pool_snapconcat.map(snapshot_concat_multi, sorted(target_obs))
		pool_concat.map(final_concat_multi, rsm_band_numbers)
	print "Done!"

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Imaging Step
#----------------------------------------------------------------------------------------------------------------------------------------------

#Loops through each group folder and launches an AWimager step or CASA
if image==True:
	#Switch to new awimager
	awimager_environ=rsmf.convert_newawimager(os.environ)
		
	# globterms=["L*/L*BAND*.MS.dppp", "SAP00*BAND*_FINAL.MS"]		
	toimage=sorted(glob.glob("L*/L*BAND*.MS.dppp"))
	
	if image_meth=="AW" or image_meth=="BOTH":
		print "Starting imaging process with AWimager..."
		image_file=open("parsets/aw.parset", 'r')
		aw_sets=image_file.readlines()
		image_file.close()
		AW_Steps_multi=partial(rsmf.AW_Steps, aw_sets=aw_sets, aw_env=awimager_environ, quiet=qu)
		if __name__ == '__main__':
			pool = Pool(processes=2)
			result = pool.map(AW_Steps_multi,toimage)
		print "Done!"
		
	if image_meth=="CASA" or image_meth=="BOTH":
		print "Starting imaging process with Casa..."
		settings=open('parsets/casa.parset', 'r')
		casa_sets=settings.readlines()
		settings.close()
		CASA_STEPS_multi=partial(rsmf.CASA_STEPS, casa_sets=casa_sets, quiet=qu)
		if __name__ == '__main__':
			pool = Pool(processes=2)
			result = pool.map(CASA_STEPS_multi,toimage)
		print "Done!"
	
#----------------------------------------------------------------------------------------------------------------------------------------------
#																End of Process
#----------------------------------------------------------------------------------------------------------------------------------------------
 
#Finishes up and moves the directory if chosen, performing checks
print "Tidying up..."
os.mkdir("FINAL_COMB_DATASETS")
subprocess.call("mv SAP00*BAND*_FINAL.MS FINAL_COMB_DATASETS", shell=True)
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
	
if image:
	for i in target_obs:
		os.chdir(i)
		subprocess.call("mkdir images", shell=True)
		subprocess.call("mv *.fits images", shell=True)
		if image_meth=="CASA" or image_meth=="BOTH":
			subprocess.call("mv *.image *.flux *.model *.residual *.psf images", shell=True)
		if image_meth=="AW" or image_meth=="BOTH":
			subprocess.call("mv *.model *.residual *.psf *.restored *.img0.avgpb *.img0.spheroid_cut *.corr images", shell=True)
		os.chdir("..")

print "All processed successfully!"
if move==True:
	subprocess.call("cp -r {0} {1}{0}".format(newdirname, mvdir), shell=True)
	if os.path.isdir("{0}{1}".format(mvdir, newdirname))==True:
		subprocess.call("rm -rf {0}".format(newdirname), shell=True)
		print "Results can be found in {0} ".format("{0}{1}".format(mvdir, newdirname), shell=True)
	else:
		print "Could not confirm that results were moved sucessfully, results have not been deleted from current directory"
else:
	print "Results can be found in {0}".format(newdirname)
	
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
	em.send_email(emacc,user_address,"slofarpp Job Completed","{0},\n\nYour job {1} has been completed - finished at {2} UTC with a runtime of {3}".format(user,newdirname, date_time_end, tdelta))

os.chdir("..")
subprocess.call(["rm", "emailslofar.py", "quick_keys.py"])
print "Run finished at {0} UTC with a runtime of {1}".format(date_time_end, str(tdelta))