import glob

# SIP Test
# to_process=sorted(glob.glob("/home/as24v07/lofar_dataB/LIGO/L2010_20502_1/*.MS.dppp"))[:8]
# to_process=sorted(glob.glob("/media/RAIDB/lofar_data/L21641/new/*.MS.dppp"))# [:20]

# HBA MSSS1 Test
# to_process=["L58535"]
# to_process=["L"+str(i) for i in range(58535,58587,2)]

# HBA MSSS2 Test
# to_process=["L"+str(i) for i in range(58535,58539)]

# LBA MSSS Test
# to_process=["L"+str(i) for i in range(41649,41665,8)]

# RSM Test
to_process=["L"+str(i) for i in range(81139,81141)]