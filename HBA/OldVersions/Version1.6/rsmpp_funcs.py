#Version 1.6

import os, subprocess,time, multiprocessing, glob, datetime, pyfits, logging, sys
import numpy as np
import pyrap.tables as pt
from collections import Counter

log=logging.getLogger("rsm")

class Ddict(dict):
	def __init__(self, default=None):
		self.default = default

	def __getitem__(self, key):
		if not self.has_key(key):
			self[key] = self.default()
		return dict.__getitem__(self, key)

class MemoryMonitor(multiprocessing.Process):
	def __init__(self, username, pipe):
		"""Create new MemoryMonitor instance."""
		multiprocessing.Process.__init__(self)
		self.username = username
		self.pipe=pipe
		self.exit = multiprocessing.Event()

	def status(self, label):
		self.store.append(label)

	def run(self):
		"""Return int containing memory used by user's processes."""
		store=[]
		while not self.exit.is_set():
			self.process = subprocess.Popen("ps -u %s -o rss,pcpu | awk '{sum+=$1} {sum2+=$2} END {print sum,sum2}'" % self.username,
			shell=True,
			stdout=subprocess.PIPE,
			)
			self.stdout_list = self.process.communicate()[0].split('\n')
			time.sleep(10)
			mem=int(self.stdout_list[0].split()[0])/1.049e6	#in GB
			cpuperc=int(float(self.stdout_list[0].split()[1]))
			store.append("{0}\t\t{1} GB\t\t{2}".format(datetime.datetime.utcnow().strftime("%d-%b-%Y %H:%M:%S"),mem,cpuperc))
		self.pipe.send(store)

	def shutdown(self):
		log.info("System monitoring shutting down...")
		self.exit.set()

#----------------------------------------------------------------------------------------------------------------------------------------------
#																Function Definitions
#----------------------------------------------------------------------------------------------------------------------------------------------

def check_targets(i, beam, targets, targets_corrupt, rsm_bands, rsm_band_numbers, rsm_bands_lens, missing_calibrators, data_dir, diff, missingfile, subsinbands):
	"""
	Checks all target observations, works out if any are missing and then organises into bands.
	"""
	beamselect="SAP00{0}".format(beam)
	log.info("Checking {0} Beam SAP00{1}...".format(i,beam))
	targlob=os.path.join(data_dir,i,"*{0}*.MS.dppp".format(beamselect))
	targets[i][beamselect]=sorted(glob.glob(targlob))
	log.debug(targets[i][beamselect])
	if len(targets[i][beamselect])<1:
		log.critical("Cannot find any measurement sets in directory {0} !".format(os.path.join(data_dir,i)))
		sys.exit()
	targets_first=int(targets[i][beamselect][0].split('SB')[1][:3])
	targets_last=int(targets[i][beamselect][-1].split('SB')[1][:3])
	target_range=range(0+(beam*diff), diff+(beam*diff))
	temp=[]
	toremove=[]
	for bnd in rsm_band_numbers:
		rsm_bands["{0}_{1}_{2}".format(i, beamselect, bnd)]=[]
	for t in targets[i][beamselect]:
		target_msname=t.split("/")[-1]
		try:
			test=pt.table(t, ack=False)
		except:
			log.warning("Target {0} is corrupt!".format(target_msname))
			time.sleep(1)
			targets_corrupt[i].append(t)
			toremove.append(t)
			missingfile.write("Measurement set {0} corrupted from observation {1}\n".format(target_msname, i))
		else:
			SB=int(t.split('SB')[1][:3])
			SB_cal=int(t.split('SB')[1][:3])-(beam*diff)
			temp.append(SB)
			if SB_cal in missing_calibrators[i]:
				toremove.append(t)
				miss=True
			else:
				miss=False
			if miss==False:
				target_bandno=int(SB_cal/subsinbands)
				rsm_bands[i+"_"+beamselect+"_{0}".format(target_bandno)].append(i+"/"+t.split("/")[-1])
				# rsm_bands_lens[i+"_"+beamselect+"_{0}".format(target_bandno)]=len(rsm_bands[i+"_"+beamselect+"_{0}".format(target_bandno)])
			# if 0 <= SB_cal < 12:
			# 	rsm_bands[i+"_"+beamselect+"_0"].append(i+"/"+t.split("/")[-1])
			# 	rsm_bands_lens[i+"_"+beamselect+"_0"]=len(rsm_bands[i+"_"+beamselect+"_0"])
			# if 12 <= SB_cal < 23:
			# 	rsm_bands[i+"_"+beamselect+"_1"].append(i+"/"+t.split("/")[-1])
			# 	rsm_bands_lens[i+"_"+beamselect+"_1"]=len(rsm_bands[i+"_"+beamselect+"_1"])
			# if 23 <= SB_cal < 34:
			# 	rsm_bands[i+"_"+beamselect+"_2"].append(i+"/"+t.split("/")[-1])
			# 	rsm_bands_lens[i+"_"+beamselect+"_2"]=len(rsm_bands[i+"_"+beamselect+"_2"])
	for s in target_range:
		if s not in temp:
			missingfile.write("Sub band {0} missing from observation {1}\n".format(s, i))
			# missing_count+=1
	log.debug("To remove = {0}".format(toremove))
	for j in toremove:
		targets[i][beamselect].remove(j)
	for k in rsm_bands:
		rsm_bands_lens[k]=len(rsm_bands[k])


def NDPPP_Initial(SB, wk_dir, ndppp_base, prec, precloc):
	"""
	Creates an NDPPP parset file using settings already supplied and adds\
	the msin and out parameters. Then runs using NDPPP and removes the parset.
	"""
	curr_SB=SB.split('/')[-1]
	curr_obs=curr_SB.split("_")[0]
	ndppp_filename='ndppp.initial.{0}.parset'.format(curr_SB)
	g = open(ndppp_filename, 'w')
	g.write("msin={0}\n".format(SB))
	if prec:
		g.write("msin.datacolumn = {0}\n".format(precloc))
		if SB[-3:]==".MS":
			g.write("msout={0}.dppp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
		else:	
			g.write("msout={0}\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
	else:
		g.write("msin.datacolumn = DATA\n")
		if SB[-3:]==".MS":
			g.write("msout={0}.dppp.tmp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
		else:	
			g.write("msout={0}.tmp\n".format(os.path.join(wk_dir, curr_obs, curr_SB)))
	for i in ndppp_base:
		g.write(i)
	g.close()
	log.info("Performing Initial NDPPP on {0}...".format(curr_SB))
	subprocess.call("NDPPP {0} > {1}/logs/ndppp.{2}.log 2>&1".format(ndppp_filename, curr_obs, curr_SB), shell=True)
	os.remove(ndppp_filename)

def check_dataset(ms):
	check=pt.table(ms, ack=False)
	try:
		row=check.row("DATA").get(0)
	except:
		log.warning("{0} is corrupt!".format(ms))
		return ms
	else:
		check.close()
		return True

def calibrate_msss1(Calib, beams, diff, calparset, calmodel, correctparset, dummy, oddeven, firstid):
	"""
	Function that performs the full calibrator calibration and transfer of solutions for HBA and LBA. Performs \
	the calibration and then shifts the corrected data over to a new data column.
	"""
	calibsplit=Calib.split('/')
	curr_obs=calibsplit[0]
	calib_name=calibsplit[-1]
	obs_number=int(curr_obs.replace("L",""))
	if oddeven=="even":
		if firstid=="even":
			tar_number=obs_number-1
		else:
			tar_number=obs_number+1
	else:
		if firstid=="even":
			tar_number=obs_number+1
		else:
			tar_number=obs_number-1
	tar_obs="L"+str(tar_number)
	curr_SB=int(Calib.split("_")[2][-3:])
	log.info("Calibrating calibrator {0}...".format(calib_name))
	subprocess.call("calibrate-stand-alone --replace-parmdb --sourcedb sky.calibrator {0} {1} {2} > {3}/logs/calibrate_cal_{4}.txt 2>&1".format(Calib,calparset,calmodel, curr_obs, calib_name), shell=True)
	log.info("Zapping suspect points for {0}...".format(calib_name))
	subprocess.call("/home/as24v07/edit_parmdb/edit_parmdb.py --sigma=1 --auto {0}/instrument/ > {1}/logs/edit_parmdb_{2}.txt 2>&1".format(Calib, curr_obs, calib_name), shell=True)
	log.info("Making diagnostic plots for {0}...".format(calib_name))
	subprocess.call("/home/as24v07/plotting/solplot.py -q -m -o {2}/{0} {1}/instrument/ > {2}/logs/solplot.log 2>&1".format(calib_name, Calib, curr_obs),shell=True)
	log.info("Obtaining Median Solutions for {0}...".format(calib_name))
	subprocess.call("parmexportcal in={0}/instrument/ out={0}.parmdb > {1}/logs/parmexportcal_{2}_log.txt 2>&1".format(Calib, curr_obs, calib_name), shell=True)
	for beam in beams:
		if beam==0:
			target=Calib.replace(curr_obs,tar_obs)
		else:
			target_subband=curr_SB+(diff*beam)
			target=Calib.replace("SB{0}".format('%03d' % curr_SB), "SB{0}".format('%03d' % target_subband)).replace("SAP000", "SAP00{0}".format(beam)).replace(curr_obs, tar_obs)
		target_name=target.split('/')[-1]
		log.info("Transferring calibrator solutions to {0}...".format(target_name))
		subprocess.call("calibrate-stand-alone --sourcedb sky.dummy --parmdb {0}.parmdb {1} {2} {3} > {4}/logs/calibrate_transfer_{5}.txt 2>&1".format(Calib, target, correctparset, dummy, curr_obs, target_name), shell=True)
		shiftndppp(target, tar_obs, target_name)


def shiftndppp(target, tar_obs, target_name):
	"""
	Simply shifts the CORRECTED_DATA to a new measurement set DATA column.
	"""
	shift_ndppp=open("ndppp.shift_{0}.parset".format(target_name), 'w')
	shift_ndppp.write("msin={0}\n\
# msin.missingdata=true\n\
# msin.orderms=false\n\
msin.datacolumn=CORRECTED_DATA\n\
msin.baseline=*&\n\
msout={1}\n\
msout.datacolumn=DATA\n\
steps=[]".format(target, target.replace(".dppp.tmp", ".dppp")))
	shift_ndppp.close()
	log.info("Performing shift NDPPP for {0}...".format(target_name))
	subprocess.call("NDPPP ndppp.shift_{0}.parset > {1}/logs/ndppp_shift_{0}.log 2>&1".format(target_name, tar_obs), shell=True)
	os.remove("ndppp.shift_{0}.parset".format(target_name))
	if os.path.isdir(target.replace(".dppp.tmp", ".dppp")):
		subprocess.call("rm -r {0}".format(target), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)


def rsm_bandsndppp(a, rsm_bands):
	"""
	Function to combine together the sub bands into bands.
	"""
	info=a.split("_")
	current_obs=info[0]
	beamc=info[1]
	b=current_obs+"_"+beamc
	band=int(info[2])
	# b_real=b+(beam*34)
	log.info("Combining {0} BAND{1}...".format(b, '%02d' % band))
	filename="{0}_ndppp.band{1}.parset".format(b, '%02d' % band)
	n=open(filename, "w")
	n.write("msin={0}\n\
msin.datacolumn=DATA\n\
msin.baseline=[CR]S*&\n\
msout={1}/{2}_BAND{3}.MS.dppp.tmp\n\
steps=[]".format(rsm_bands[a], current_obs, b,'%02d' % band))
	n.close()
	subprocess.call("NDPPP {0} > {1}/logs/{2}_BAND{3}.log 2>&1".format(filename,current_obs,b,'%02d' % band), shell=True)
	os.remove(filename)

def calibrate_msss2(target, phaseparset, flag, toflag, autoflag, create_sky, skymodel):
	"""
	Function for the second half of MSSS style calibration - it performs a phase-only calibration and the auto flagging \
	if selected.
	"""
	tsplit=target.split("/")
	curr_obs=tsplit[0]
	name=tsplit[-1]
	beam=target.split("_")[1]
	if create_sky==True:
		skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Performing phase only calibration on {0}...".format(target))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {3}/logs/calibrate_phase_{4}.txt 2>&1".format(target, phaseparset, skymodel, curr_obs, name), shell=True)
	if flag==True:
		local_toflag=toflag
		if autoflag==True:
			final_toflag=flagging(target, local_toflag)[:-1]
		log.info("Flagging baselines: {0} from {1}".format(final_toflag, target))
	else:
		final_toflag=""
	subprocess.call('msselect in={0} out={1} baseline=\'{2}\' deep=true > {3}/logs/msselect.log 2>&1'.format(target, target.replace(".tmp", ""), final_toflag, curr_obs), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)
	if os.path.isdir(target.replace(".tmp", "")):
		subprocess.call("rm -rf {0}".format(target), shell=True)


def standalone_phase(target, phaseparset, flag, toflag, autoflag, create_sky, skymodel, phaseoutput, phasecolumn):
	"""
	Simply shifts the CORRECTED_DATA to a new measurement set DATA column.
	"""
	tsplit=target.split("/")
	target_name=tsplit[-1]
	curr_obs=tsplit[0]
	beam=target.split("_")[1]
	phase_shift_ndppp=open("ndppp.shift_{0}.parset".format(target_name), 'w')
	phase_shift_ndppp.write("msin={0}\n\
msin.datacolumn={1}\n\
msin.baseline=*&\n\
msout={0}.PHASEONLY.tmp\n\
msout.datacolumn=DATA\n\
steps=[]".format(target, phasecolumn))
	phase_shift_ndppp.close()
	log.info("Performing phase shift NDPPP for {0}...".format(target_name))
	subprocess.call("NDPPP ndppp.shift_{0}.parset > {1}/logs/ndppp_phase_standalone_shift_{0}.log 2>&1".format(target_name, curr_obs), shell=True)
	os.remove("ndppp.shift_{0}.parset".format(target_name))
	target+=".PHASEONLY.tmp"
	if create_sky:
		skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Performing phase only calibration on {0}...".format(target))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {3}/logs/calibrate_standalone_phase_{4}.txt 2>&1".format(target, phaseparset, skymodel, curr_obs, target_name), shell=True)
	if flag:
		local_toflag=toflag
		if autoflag:
			final_toflag=flagging(target, local_toflag)[:-1]
		log.info("Flagging baselines: {0} from {1}".format(final_toflag, target))
	else:
		final_toflag=""
	subprocess.call('msselect in={0} out={1} baseline=\'{2}\' deep=true > {3}/logs/msselect_phaseonly.log 2>&1'.format(target, os.path.join(curr_obs, phaseoutput,target_name+".PHASEONLY"),final_toflag,curr_obs), shell=True)
	subprocess.call("mv calibrate-stand-alone*log logs > logs/movecalibratelog.log 2>&1", shell=True)
	if os.path.isdir(os.path.join(curr_obs, phaseoutput,target_name+".PHASEONLY")):
		subprocess.call("rm -rf {0}".format(target), shell=True)
	subprocess.call("mv {0}*.pdf {0}*.stats {0}*.tab {1}/flagging/".format(target, curr_obs), shell=True)


def flagging(target, local_toflag):
	"""
	A function which copies the auto detection of bad stations developed during MSSS.
	"""
	log.info("Gathering AutoFlag Information for {0}...".format(target))
	subprocess.call('~as24v07/plotting/asciistats.py -i {0} -r {1}/ > {1}/logs/asciistats.log 2>&1'.format(target, target.split("/")[0]), shell=True)
	subprocess.call('~as24v07/plotting/statsplot.py -i {0}.stats -o {0} > logs/statsplot.log 2>&1'.format(target), shell=True)
	stats=open('{0}.tab'.format(target), 'r')
	for line in stats:
		if line.startswith('#')==False:
			cols=line.rstrip('\n').split('\t')
			if cols[12] == 'True':
				if cols[12] not in local_toflag:
					local_toflag+=('!'+cols[1]+';')
	return local_toflag

def peeling_steps(SB, shortpeel, peelsources, peelnumsources, fluxlimit):
	"""
	Performs the peeling steps developed during MSSS activities.
	"""
	peelsplit=SB.split('/')
	logname=peelsplit[-1]
	obsid=peelsplit[0]
	prepeel=logname+".prepeel"
	beam=logname.split("_")[1]
	skymodel="parsets/{0}.skymodel".format(beam)
	log.info("Creating new {0} dataset ready for peeling...".format(SB))
	p_shiftname="peeling_shift_{0}.parset".format(logname)
	f=open(p_shiftname, 'w')
	f.write("msin={0}\n\
msin.datacolumn=CORRECTED_DATA\n\
# msin.baseline=*&\n\
msout={0}.peeltmp\n\
# msout.datacolumn=DATA\n\
steps=[]".format(SB))
	f.close()
	subprocess.call("NDPPP {0} > {1}/logs/ndppp_peeling_shift_{2}.log 2>&1".format(p_shiftname, obsid, logname), shell=True)
	peelparset=SB+"_peeling.parset"
	if shortpeel:
		log.info("Performing only first stage of peeling (i.e. peeled sources will not be re-added)")
		subprocess.call(['cp', '/home/as24v07/scripts/peeling/parsets/peeling_new.parset', peelparset])
	else:
		log.info("Performing full peeling steps")
		peel2parset=SB+'_peeling_step2.parset'
		subprocess.call(['cp', '/home/as24v07/scripts/peeling/parsets/peeling_new_readyforstep2.parset', peelparset])
		subprocess.call(['cp', '/home/as24v07/scripts/peeling/parsets/peeling_new_step2.parset', peel2parset])
	log.info("Determining sources to peel for {0}...".format(SB))
	if peelsources=="0":
		subprocess.call("/home/as24v07/scripts/peeling/peeling_new_slofarpp.py -i {0} -p {1} -m {2} -v -n {3} -l {4}".format(SB, peelparset, skymodel, peelnumsources, fluxlimit), shell=True)
	else:
		subprocess.call("/home/as24v07/scripts/peeling/peeling_new_slofarpp.py -i {0} -p {1} -m {2} -v -n {3} -s {4} -l {5}".format(SB, peelparset, skymodel, peelnumsources, peelsources, fluxlimit), shell=True)
	newSB=SB+".peeltmp"
	log.info("Peeling {0}...".format(SB))
	subprocess.call("calibrate-stand-alone -f {0} {1} {2} > {4}/logs/{3}_peeling_calibrate.log 2>&1".format(newSB, peelparset, skymodel, logname, obsid), shell=True)
	if not shortpeel:
		subprocess.call("/home/as24v07/scripts/peeling/float_solutions.py -f -o {0}.skymodel {0}/instrument/ {1} > {3}/logs/{2}_float_solutions.txt 2>&1".format(newSB, skymodel, logname, obsid), shell=True)
		subprocess.call("calibrate-stand-alone -f {0} {1} {0}.skymodel > {3}/logs/{2}_peeling_calibrate_step2.log 2>&1".format(newSB, peel2parset, logname, obsid), shell=True)
	#move preepeeled dataset
	subprocess.call('msselect in={0} out={2}/prepeeled_sets/{1} deep=true > {2}/logs/msselect_moveprepeel.log 2>&1'.format(SB, prepeel, obsid), shell=True)
	#rename the peeled dataset
	subprocess.call('msselect in={0} out={1} deep=true > {2}/logs/msselect_movingpeeled.log 2>&1'.format(newSB, SB, obsid), shell=True)
	if os.path.isdir(SB):
		subprocess.call("rm -r {0}.peeltmp".format(SB), shell=True)
	os.remove(p_shiftname)
	os.remove(peelparset)
	if not shortpeel:
		os.remove(peel2parset)
		os.remove("{0}.skymodel".format(newSB))

def post_bbs(SB, postcut):
	"""
	Generates a standard NDPPP parset and clips the amplitudes to user specified level.
	"""
	SBsplit=SB.split('/')
	SB_name=SBsplit[-1]
	log.info("Performing post-BBS NDPPP flagging, with cut of {0}, on {1}...".format(postcut, SB_name))
	postbbsfname='ndppp.{0}.postbbs.parset'.format(SB_name)
	ndppp_postbbs=open(postbbsfname,'w')
	ndppp_postbbs.write("msin={0}\n\
msin.datacolumn = CORRECTED_DATA\n\
msout=\n\
msout.datacolumn = CORRECTED_DATA\n\
\n\
steps = [preflag]   # if defined as [] the MS will be copied and NaN/infinite will be  flagged\n\
\n\
preflag.type=preflagger\n\
preflag.corrtype=cross\n\
preflag.amplmax={1}\n\
preflag.baseline=[CS*,RS*,DE*,SE*,UK*,FR*]".format(SB, postcut))
	ndppp_postbbs.close()
	subprocess.call("NDPPP ndppp.{0}.postbbs.parset > {1}/logs/ndppp_postbbs_{0}.txt 2>&1".format(SB_name, SBsplit[0]), shell=True)
	os.remove(postbbsfname)


# def snapshot_concat(i, beam):
# 	"""
# 	Simply uses concat.py to concat all the BANDX together in each snapshot.
# 	"""
# 	log.info("Combining {0} SAP00{1} datasets...".format(i,beam))
# 	subprocess.call("~as24v07/scripts/concat.py {0}/{0}_SAP00{1}_ALLBANDS.MS {0}/L*SAP00{1}_BAND??.MS.dppp > {0}/logs/concat_SAP00{1}_allbands.log 2>&1".format(i,beam), shell=True)

def final_concat(band, beam, target_obs, correct):
	"""
	Simply uses concat.py to concat all the BANDX together into a final set.
	"""
	log.info("Concatenating BEAM {0} BAND{1:02d}".format(beam, band))
	concat_commd="/home/as24v07/scripts/concat.py SAP00{0}_BAND{1:02d}_FINAL.MS.dppp".format(beam,band)
	toconcat=sorted(glob.glob("L*/*SAP00{0}*BAND{1:02d}*.dppp".format(beam, band)))
	for ms in toconcat:
		temp=pt.table("{0}/SPECTRAL_WINDOW".format(ms), ack=False)
		nchans=int(temp.col("NUM_CHAN")[0])
		if nchans == correct:
			concat_commd+=" {0}".format(ms)
		else:
			log.error("MS {0} has less than {1} channels - skipping in concat...".format(ms, correct))
	subprocess.call(concat_commd+" > logs/concat_SAP00{0}_BAND{1:02d}.log 2>&1".format(beam, band), shell=True)
# 	datasetstocon=sorted(glob.glob("L*/L*SAP00{0}_BAND{1}.MS.dppp".format(beam,'%02d' % b)))
# 	numbers=[]
# 	datasets={}
# 	for i in target_obs:
# 		num=rsm_bands_lens[i+"_"+"SAP00{0}".format(beam)+"_{0}".format(b)]
# 		numbers.append(num)
# 		datasets[i]=num
# 	cnt=Counter()
# 	for a in numbers:
# 		cnt[a]+=1
# 	correct=int(cnt.most_common(1)[0][0])
# 	for i in datasets:
# 		if datasets[i]!=correct:
# 			datasetstocon.remove("{0}/{0}_SAP00{1}_BAND{2}.MS.dppp".format(i, beam, '%02d' % b))
# 	concat_commd="/home/as24v07/scripts/concat.py SAP00{0}_BAND{1}_FINAL.MS".format(beam,'%02d' % b)
# 	for i in datasetstocon:
# 		concat_commd+=" {0}".format(i)
# 	concat_commd+=" > logs/concat_SAP00{0}_band{1}.log 2>&1".format(beam,'%02d' % b)
# 	if not quiet:
# 		print "Combining Final BEAM {0} BAND{1}...".format(beam, '%02d' % b)
# 	subprocess.call(concat_commd, shell=True)


def CASA_STEPS(g, casa_sets):
	"""
	Performs imaging with CASA using user supplied settings.
	"""
	casafile_name='casasteps_{0}.py'.format(g)
	casafile=open(casafile_name, 'w')
	casafile.write("default('clean')\n")
	casafile.write("vis='{0}'\n\
imagename='{0}_CASA'\n".format(g))
	for line in casa_sets:
		casafile.write(line)
	casafile.write("\nclean()")
	casafile.close()
	log.info("Imaging {0} with casa...".format(g))
	subprocess.call("casapy --nologger -c {0} > logs/casa_{1}_log.txt 2>&1".format(casafile_name, g), shell=True)
	subprocess.call("image2fits in={0}_CASA.image out={0}_CASA.fits > logs/image2fits.log 2>&1".format(g), shell=True)
	os.remove(casafile_name)


def convert_newawimager(environ):
	"""
	Returns an environment that utilises the new version of the AWimager for rsm-mainline.
	"""
	environ['LOFARROOT']="/opt/share/lofar-archive/2013-02-11-16-46/LOFAR_r_b0fc3f4"
	environ['PATH']="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/opt/share/soft/pathdirs/bin:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/bin"
	environ['LD_LIBRARY_PATH']="/opt/share/soft/pathdirs/lib:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/lib"
	environ['PYTHONPATH']="/opt/share/soft/pathdirs/python-packages:/opt/share/lofar-archive/2013-02-11-16-46/pathdirs/python-packages"
	return environ

def create_mask(beam, mask_size, toimage):
	beamc="SAP00{0}".format(beam)
	mask="parsets/{0}.mask".format(beamc)
	for i in toimage:
		if beamc in i:
			g=i
			break
	if not os.path.isdir(mask):
		log.info("Creating {0} mask...".format(beamc))
		skymodel="parsets/{0}.skymodel".format(beamc)
		subprocess.call('makesourcedb in={0} out={0}.temp format=Name,Type,Ra,Dec,I,Q,U,V,ReferenceFrequency=\\\"60e6\\\",SpectralIndex=\\\"[0.0]\\\",MajorAxis,MinorAxis,Orientation > /dev/null 2>&1'.format(skymodel), shell=True)
		mask_command="awimager ms={0} image={1} operation=empty stokes='I'".format(g, mask)
		mask_command+=mask_size
		subprocess.call(mask_command+" > logs/aw_mask_creation_{0}.log 2>&1".format(beamc), shell=True)
		subprocess.call("/home/as24v07/scripts/msss_mask.py {0} {1}.temp > logs/msss_mask.log 2>&1".format(mask, skymodel), shell=True)
		subprocess.call(["rm", "-r", "{0}.temp".format(skymodel)])

def AW_Steps(g, aw_sets, maxb, aw_env, niter, automaticthresh, bandsthreshs_dict, initialiter, uvORm, userthresh, nomask, mos):
	"""
	Performs imaging with AWimager using user supplied settings.
	"""
	c=299792458.
	if g.find("/"):
		logname=g.split("/")[-1]
	else:
		logname=g
	obsid=logname.split("_")[0]
	ft = pt.table(g+'/SPECTRAL_WINDOW', ack=False)
	freq = ft.getcell('REF_FREQUENCY',0)
	wave_len=c/freq
	if uvORm == "M":
		UVmax=maxb/(wave_len*1000.)
		localmaxb=maxb
	else:
		UVmax=maxb
		localmaxb=UVmax*wave_len*1000.
	ft.close()
	log.debug("Frequency = {0} Hz".format(freq))
	log.debug("Wavelength = {0} m".format(wave_len))
	log.info("UVmax = {0}".format(UVmax))
	beam=int(g.split("SAP")[1][:3])
	beamc="SAP00{0}".format(beam)
	finish_iters=niter
	if automaticthresh:
		# finish_iters+=initialiter
		curr_band=g.split("BAND")[1][:2]
		aw_parset_name="aw_{0}.parset".format(g.split("/")[-1])
		local_parset=open(aw_parset_name, 'w')
		local_parset.write("\nms={0}\n\
image={0}.img\n\
niter={1}\n\
threshold={2}Jy\n\
UVmax={3}\n".format(g, initialiter, 6.*bandsthreshs_dict[curr_band],UVmax))
		if not nomask:
			mask="parsets/{0}.mask".format(beamc)
			local_parset.write("mask={0}\n".format(mask))
		for i in aw_sets:
			local_parset.write(i)
		local_parset.close()
		log.info("Imaging {0} with AWimager...".format(g))
		subprocess.call("awimager {0} > {1}/logs/awimager_{2}_initial_log.txt 2>&1".format(aw_parset_name, obsid, logname), env=aw_env, shell=True)
		subprocess.call("image2fits in={0}.img.residual out={0}.img.fits > {1}/logs/image2fits.log 2>&1".format(g, obsid), shell=True)
		try:
			thresh=2.5*(getimgstd("{0}.img.fits".format(g)))
		except:
			log.error("FITS {0}.img.fits could not be found!".format(g))
			return
		os.remove("{0}.img.fits".format(g))
	else:
		thresh=userthresh
	log.info("Cleaning {0} to threshold of {1}...".format(g, thresh))
	local_parset=open(aw_parset_name, 'w')
	local_parset.write("\nms={0}\n\
image={0}.img\n\
niter={1}\n\
threshold={2}Jy\n\
UVmax={3}\n".format(g, finish_iters, thresh, UVmax))
	if not nomask:
		local_parset.write("mask={0}\n".format(mask))
	for i in aw_sets:
		local_parset.write(i)
	local_parset.close()
	subprocess.call("awimager {0} > {1}/logs/awimager_{2}_final_log.txt 2>&1".format(aw_parset_name, obsid, logname), env=aw_env, shell=True)
	subprocess.call("image2fits in={0}.img.restored.corr out={0}.img.fits > {1}/logs/image2fits.log 2>&1".format(g, obsid), shell=True)
	if mos:
		subprocess.call("cp -r {0}.img.restored.corr {0}.img_mosaic.restored.corr".format(g), shell=True)
		subprocess.call("cp -r {0}.img0.avgpb {0}.img_mosaic0.avgpb".format(g), shell=True)
	subprocess.call("addImagingInfo {0}.img.restored.corr '' 0 {3} {0} > {1}/logs/addImagingInfo_{2}_log.txt 2>&1".format(g, obsid, logname, localmaxb), shell=True)
	os.remove(aw_parset_name)


def getimgstd(infile):
	fln=pyfits.open(infile)
	rawdata=fln[0].data
	angle=fln[0].header['obsra']
	bscale=fln[0].header['bscale']
	rawdata=rawdata.squeeze()
	rawdata=rawdata*bscale
	while len(rawdata) < 20:
		rawdata = rawdata[0]
	X,Y = np.shape(rawdata)
	rawdata = rawdata[Y/6:5*Y/6,X/6:5*X/6]
	orig_raw = rawdata
	med, std, mask = Median_clip(rawdata, full_output=True, ftol=0.0, max_iter=10, sigma=3)
	rawdata[mask==False] = med
	fln.close()
	return std

def Median_clip(arr, sigma=3, max_iter=3, ftol=0.01, xtol=0.05, full_output=False, axis=None):
    """Median_clip(arr, sigma, max_iter=3, ftol=0.01, xtol=0.05, full_output=False, axis=None)
    Return the median of an array after iteratively clipping the outliers.
    The median is calculated upon discarding elements that deviate more than
    sigma * standard deviation the median.

    arr: array to calculate the median from.
    sigma (3): the clipping threshold, in units of standard deviation.
    max_iter (3): the maximum number of iterations. A value of 0 will
        return the usual median.
    ftol (0.01): fraction tolerance limit for convergence. If the number
        of discarded elements changes by less than ftol, the iteration is
        stopped.
    xtol (0.05): absolute tolerance limit for convergence. If the number
        of discarded elements increases above xtol with respect to the
        initial number of elements, the iteration is stopped.
    full_output (False): If True, will also return the indices that were good.
    axis (None): Axis along which the calculation is to be done. NOT WORKING!!!

    >>> med = Median_clip(arr, sigma=3, max_iter=3)
    >>> med, std, inds_good = Median_clip(arr, sigma=3, max_iter=3, full_output=True)
    """
    arr = np.ma.masked_invalid(arr)
    med = np.median(arr, axis=axis)
    std = np.std(arr, axis=axis)
    ncount = arr.count(axis=axis)
    for niter in xrange(max_iter):
        ncount_old = arr.count(axis=axis)
        if axis is not None:
            condition = (arr < np.expand_dims(med-std*sigma, axis)) + (arr > np.expand_dims(med+std*sigma, axis))
        else:
            condition = (arr < med-std*sigma) + (arr > med+std*sigma)
        arr = np.ma.masked_where(condition, arr)
        ncount_new = arr.count(axis)
        med = np.median(arr, axis=axis)
        std = np.std(arr, axis=axis)
        if np.any(ncount-ncount_new > xtol*ncount):
            print( "xtol reached {}; breaking at iteration {}".format(1-1.*ncount_new/ncount, niter+1) )
            break
        if np.any(ncount_old-ncount_new < ftol*ncount_old):
            print( "ftol reached {}; breaking at iteration {}".format(1-1.*ncount_new/ncount_old, niter+1) )
            break
    if full_output:
        if isinstance(arr.mask, np.bool_):
            mask = np.ones(arr.shape, dtype=bool)
        else:
            mask = ~arr.mask
        if axis is not None:
            med = med.data
            std = std.data
        return med, std, mask
    if axis is not None:
        med = med.data
    return med

def average_band_images(snap, beams):
	for b in beams:
		log.info("Averaging {0} SAP00{1}...".format(snap, b))
		subprocess.call("/home/as24v07/scripts/average_inverse_var3.py {0}/images/{0}_SAP00{1}_AVG {0}/images/{0}_SAP00{1}_BAND0?.MS.dppp.img.fits > {0}/logs/average_SAP00{1}_log.txt 2>&1".format(snap, b), shell=True)
	
def create_mosaic(snap, band_nums, chosen_environ, pad):
	for b in band_nums:
		tocorrect=sorted(glob.glob(os.path.join(snap, "images","L*_SAP00?_BAND0{0}.MS.dppp.img_mosaic0.avgpb".format(band_nums))))
		for w in tocorrect:
			wname=w.split("/")[-1]
			if chosen_environ=='rsm-mainline' and pad > 1.0:
				log.info("Correcting {0} mosaic padding...".format(wname))
				avgpb=pt.table("{0}".format(w), ack=False, readonly=False)
				coordstable=avgpb.getkeyword('coords')
				coordstablecopy=coordstable.copy()
				value1=coordstablecopy['direction0']['crpix'][0]
				value2=coordstablecopy['direction0']['crpix'][1]
				value1*=pad
				value2*=pad
				# value1=960.0
				# value2=960.0
				newcrpix=np.array([value1, value2])
				coordstablecopy['direction0']['crpix']=newcrpix
				avgpb.putkeyword('coords', coordstablecopy)
				avgpb.close()
			log.info("Zeroing corners of avgpb {0}...".format(wname))
			subprocess.call("python /home/as24v07/scripts/avgpbz.py {0} > {1}/logs/avgpbz_{2}_log.txt 2>&1".format(w, snap, wname), shell=True)
		tomosaic=sorted(glob.glob(os.path.join(snap, "{0}_SAP00?_BAND0{1}.MS.dppp".format(snap,b))))
		log.info("Creating {0} BAND0{1} Mosaic...".format(snap, b))
		m_list=[i.split("/")[0]+"/images/"+i.split("/")[-1]+".img_mosaic" for i in tomosaic]
		m_name=os.path.join(snap, "images", "{0}_BAND0{1}_mosaic.fits".format(snap, b))
		m_sens_name=os.path.join(snap, "images", "{0}_BAND0{1}_mosaic_sens.fits".format(snap, b))
		subprocess.call("python /home/as24v07/scripts/mos.py -o {0} -a avgpbz -s {1} {2} > {3}/logs/mosaic_band0{4}_log.txt 2>&1".format(m_name, m_sens_name, ",".join(m_list), snap, b), shell=True)

correct_lofarroot={'/opt/share/lofar-archive/2013-06-20-19-15/LOFAR_r23543_10c8b37':'rsm-mainline', '/opt/share/lofar/2013-09-09-14-41/LOFAR_r26426_7ab6b79':'lofar-sept2013'}