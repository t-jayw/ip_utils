import sys, time, pprint, re
import pandas as pd
import numpy as np
import argparse as ap
from ipaddress import IPv4Address as ipv4
from ipaddress import AddressValueError
from time import sleep

def ip_to_dec(ipstring):
	"""convert an octet ip into decimal
	returns decimal or None"""
	try:
		dec = int(ipv4(unicode(ipstring)))
		return dec
	except AddressValueError:
		return None

def ip_to_oct(ipstring):
	"""convert a decimal ip to oct"""
	try:
		oct = str(ipv4(ipstring))
		return oct
	except any as e:
		print e
		sys.exit()

def load_csv(filepath):
	print('Loading %s\n'%(filepath))
	try:
		df = pd.read_csv(filepath)
		return df
	except any as e:
		print e 

def translate_ips(file_path, direction, output, test=False):
	df = load_csv(file_path)
	if direction == 'to_dec':
		df['translated'] = df['Source IPs'].apply(ip_to_dec)
	elif direction == 'to_oct':
		df['translated'] = df['Source IPs'].apply(ip_to_oct)

	path = '%s_ip_oct_and_dec.csv'%(output)
	print('writing %s'%(path))
	df.to_csv(path)

if __name__ == '__main__':
  parser = ap.ArgumentParser(description = """""")
  parser.add_argument('ips', help="CSV of IPs ONE COLUMN" , type=str)
  parser.add_argument('direction', help='to_dec or to_oct', type=str)
  parser.add_argument('out', help='output file headers', type=str)
  parser.add_argument('-t','--test', action="store_true")
  args = parser.parse_args()

  translate_ips(args.ips, direction, out)
