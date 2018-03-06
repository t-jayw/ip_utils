### FIRST ARG MUST BE CSV WITH COLUMNS: 'Source IP', 'Login Time (Central Daylight Time)'
###  					'Status', 'Country'
### You will get an output CSV with the pivot table

import sys, time, pprint, re
import pandas as pd
import numpy as np
import argparse as ap
from ipaddress import IPv4Address as ipv4
from ipaddress import AddressValueError
from time import sleep

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

def col_to_datetime(df, time_col=DATECOL):
  if time_col in df.columns:
    print('changing %s column to datetime'%(time_col))
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

def create_pivot(ip_login_records, status, date, output):
	df = load_csv(ip_login_records)
	df = filter_by_status(df, status)
	df = col_to_datetime(df)
	df = filter_by_time_col(df, date)

	df = df[['Country', 'Source IP', 'Status']].groupby(['Country', 'Source IP']).count()

	path = '%s_status_country_ip_pivot.csv'%(output)
	print('writing %s\n'%(path))
	df.to_csv(path)

if __name__ == '__main__':
  parser = ap.ArgumentParser(description = """""")
  parser.add_argument('ip', help="CSV of IP login records." , type=str)
  parser.add_argument('status', help="Status to filter by" , type=str)  
  parser.add_argument('d', help='date to filter by >=', type=str)
  parser.add_argument('out', help='output file titles', type=str)
  parser.add_argument('-t','--test', action="store_true")
  args = parser.parse_args()

  create_pivot(args.ip, args.status, args.d, args.out)
