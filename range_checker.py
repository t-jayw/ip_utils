### Example execution
### >python range_checker.py ip_login_records.csv metadata.csv 2018-01-01 output
### ip_login_records.csv must be a csv with columns: Status, Source IP, Login Time (Central Daylight Time)
### metadata.csv must have 'IP Start Address' and 'IP End Addresss' as oct strings
### Source IP MUST be oct string

import sys, time, pprint, re
import pandas as pd
import numpy as np
import argparse as ap
from ipaddress import IPv4Address as ipv4
from ipaddress import AddressValueError
from time import sleep

strf_1 = '%m/%d/%y %H:%M'
strf_2 = '%m/%d/%Y %I:%M %p'
STRFS = [strf_1, strf_2]

DATECOL = 'Login Time (Central Daylight Time)'

def ip_to_dec(ipstring):
	"""convert an octet ip into decimal
	returns decimal or None"""
	try:
		dec = int(ipv4(unicode(ipstring)))
		return dec
	except AddressValueError:
		return None

def load_csv(filepath):
	print('Loading %s\n'%(filepath))
	try:
		df = pd.read_csv(filepath)
		return df
	except any as e:
		print e 

def filter_by_status(df, status = 'Success'):
	print('Filtering IP records by status...')
	start_len = len(df)
	df = df[df['Status']==status]
	print('IP records reduced from %i to %i'%(start_len, len(df)))
	return df

def col_to_datetime(df, time_col=DATECOL, strfs=STRFS):
  if time_col in df.columns:
    print('changing %s column to datetime'%(time_col))
    for form in strfs:
      try:
        df[time_col] = pd.to_datetime(df[time_col], format = form)
      except ValueError:
        pass
    if df[time_col].dtype ==  np.datetime64:
      return df
    else:
      print('inferring datetime format')
      df[time_col] = pd.to_datetime(df[time_col], infer_datetime_format=True)
      return df
  else:
    print('specified time column not found')
    sys.exit()
  print '\n'	

def filter_by_time_col(df, gt_date, time_col=DATECOL):
  start_len = len(df)
  gt_date = pd.to_datetime(gt_date)
  df = df[df[time_col] >= gt_date]
  print('filtered from %i to %i by date\n'%(start_len, len(df)))
  return df

def parse_range(meta_df):
    range_zip = zip([ip_to_dec(x) for x in meta_df['IP Start Address']],
                  [ip_to_dec(x)+1 for x in meta_df['IP End Address']])
    range_dict = dict((el, 0) for el in range_zip)
    return range_dict

def translate_rangedict(rangedict):
	return {(str(ipv4(name[0])),str(ipv4(name[1]))): val for name, val in rangedict.items()}

def print_progress(iteration, total, start_time='', prefix='', suffix='', decimals=1, bar_length=75):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '|' * filled_length + '-' * (bar_length - filled_length)

    if start_time:
    	curr_time = str_format.format(time.time()-start_time)

    suffix = suffix + ' %s sec' % (curr_time)

    sys.stdout.write('\r%s|%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def check_ip_vs_meta(ip_oct_series, meta_range):
	start_time = time.time()
	print('Starting the big check')
	l = len(ip_oct_series)
	print_progress(0, l, start_time, prefix = 'Progress:', suffix = 'Complete', bar_length = 40)
 	completed = 0
	errors = 0
	for ip in ip_oct_series:
		if completed % 50 == 0:
			#suffix = suffix + str(time.time()-start_time)
			print_progress(completed, l, start_time, prefix = 'Progress:', suffix = 'Complete', bar_length = 40)
		ip_dec = ip_to_dec(ip)
		if not ip_dec:
			errors += 1
			pass
		for key in meta_range.keys():
			checkrange = range(key[0], (key[1]+1))
			if ip_dec in checkrange:
				meta_range[key] += 1
		completed += 1
	return completed, errors

def range_check(ip_file, meta_file, date_str, outputs, test = False):
	
	ips = load_csv(ip_file)
	meta = load_csv(meta_file)

	if test:
		ips = ips[:1000]

	ips = filter_by_status(ips)
	ips = col_to_datetime(ips)
	ips = filter_by_time_col(ips, date_str)

	dec_meta_range = parse_range(meta)

	completed, errors = check_ip_vs_meta(ips['Source IP'], dec_meta_range)

	oct_meta_range = translate_rangedict(dec_meta_range)

	val_counts = ips['Source IP'].value_counts()

	
	# Writes
	print('\n\nWriting to files...')
	meta_df = pd.DataFrame(oct_meta_range.items())
	log = "Errors: %i\n Completed: %i\n"
	log += "Command: %s\n"%(str(sys.argv))
	names = ['metaRangeCounts.csv', 'ipValCount.csv']
	writeouts = [meta_df, val_counts]
	files = {k:v for (k,v) in zip(names, writeouts)}

	for k, v in files.items():
		path = '%s_%s'%(outputs, k)
		print('writing %s'%(path))
		v.to_csv(path)

	with open('%s_log.txt'%(outputs), 'w') as f:
		print('writing %s_log.txt'%(outputs))
		f.write(log)
		f.close()
    
if __name__ == '__main__':
  parser = ap.ArgumentParser(description = """""")
  parser.add_argument('ip', help="CSV of IP login records." , type=str)
  parser.add_argument('meta', help='CSV of IP range metadata', type=str)
  parser.add_argument('d', help='date to filter by >=', type=str)
  parser.add_argument('out', help='output file titles', type=str)
  parser.add_argument('-t','--test', action="store_true")
  args = parser.parse_args()
  
  range_check(args.ip, args.meta, args.d, args.out)




