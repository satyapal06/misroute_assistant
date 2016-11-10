import csv
import pprint
from operator import itemgetter
from fuzzywuzzy import fuzz
from pymongo import MongoClient
from Source import formatting

# Global declaration for opening a connection to the mongo server
client = MongoClient()                  # Client
my_db = client.custdb                   # Database name is "custdb"
col = my_db.cust_details               # Collection name is "cust_details"
# The main function closes the connection at the end of the program


def readCustomerDataFromCSV(filename, records_list):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	# ARGUMENTS:
	#       "filename": This is the name of the file from where data has to read
	#       "records_list": This is an empty list into which newly read data will be added
	# Its better to call the records_list by reference than to return it
	data = open(filename, 'rU')
	reader = csv.reader(data)
	keys = reader.next()
	for row in reader:
		record_dict = dict(zip(keys, row))
		# Sometimes, there are cases where the PD doesn't contain any
		# value in the 'CN' column. So we are filtering out those cases.
		# No calculation will be done on such rows.
		if record_dict['cn']:
			records_list.append(record_dict)
	data.close()


def writeToCSV(filename, contents_to_write):
	# PURPOSE:
	#       This function reads the contents of a List of Dictionaries,
	#       and stores them in a csv file.
	myKeys = [
		"wbn",
		"Misroute?",
		"Matched_adr",
		"DC_Found",
		"cn",
		"pin",
		"cl",
		"aseg.invalid_add_code",
		"date.mnd",
		"aseg.city_identified",
		"add",
		"nsl.lc",
		"aseg.loc",
		"cnc",
		"ph",
		"pl",
		"cs.st",
		"cpin",
		"cs.sr",
		"cs.ss",
		"cs.sl",
		"cs.sd",
		"nsl.code",
		"aseg.pin",
		"cty",
		"aseg.invalid_add",
		"aseg.mismatch",
		"pdd",
		"pd"
	]
	with open(filename, 'wb') as f:
		w = csv.DictWriter(f, myKeys)
		w.writeheader()
		for row in contents_to_write:
			try:
				w.writerow(row)
			except UnicodeEncodeError as e:
				continue
	return True


def findMatch(data_to_search, filename, dc_current_status):
	list_to_write = []
	i = 0
	index = 0
	dc_count = 0
	phone_not_found_count = 0
	phone_numbers_found = 0
	for row in data_to_search:
		i += 1
		# Initilaizing the new column values for each row
		row['DC_Found'] = ''
		row['Matched_adr'] = ''
		# row['Confidence'] = ''
		row['Misroute?'] = ''
		# This dictionary will contain all the DCs that have delivered
		# the order previously for that particular address
		dc_found_list = {}
		total_adr_matched = 0

		# Print initial stats
		print "\r\rFinding document: [%s / %s] [%s %%]" % (i, (len(data_to_search)), (round((i*100)/len(data_to_search), 1))),

		# Fetching all the formatted phone numbers present in the row
		phone_numbers = formatting.formatPhone(row['ph'])

		# Iterating the phone numbers and checking if any of the
		# phone numbers have the same or partially matching address
		# in the order history
		for phone in phone_numbers:
			mongoDoc = findMongoDocument(phone)

			# If phone not found in mongodb
			if not mongoDoc:
				phone_not_found_count += 1
				# Continue to check if the next phone number in the list is present in mongodb
				continue

			# If phone number found, then sort the orders and find
			# the nearest matching address and its corresponding DC
			phone_numbers_found += 1
			confidence_threshold = 75

			# #  Sorting all the orders based upon cs_sd (Delivery date)
			# for order in mongoDoc['shipment_details']:
			# 	# if order['cs_st'].lower() == 'dl' and order['cs_ss'].lower() =='delivered':
			# 	if order['cs_sd']:
			# 		order['cs_sd'] = str(order['cs_sd'])[0:10]
			# # The orders are sorted in descending order and stored in a new list
			# newlist = sorted(mongoDoc['shipment_details'], key=itemgetter('cs_sd'), reverse=True)

			latest_order_found = False
			for order in mongoDoc['shipment_details']:
				address = formatting.formatAddress(row['add'])
				if (
					(fuzz.token_set_ratio(address, order['add']) >= confidence_threshold)
				):
					if " (" in str(order['cn']):
						end_index = str(order['cn']).index(" (")
					else:
						end_index = len(str(order['cn']))
					if str(order['cn'])[0: end_index].lower() in dc_current_status.keys():
						# The DC should be active.
						if dc_current_status[str(order['cn'])[0: str(order['cn']).index(" (")].lower()] == 'active':
							dc_count += 1
							row['Matched_adr'] = order['add']
							dc_found_list.setdefault(order['cn'], 0)
							dc_found_list[order['cn']] += 1
							total_adr_matched += 1

		# After we have checked all the phone numbers, append the
		# row to the final list with the four new columns
		if dc_found_list :
			new_list = []
			for dc in dc_found_list:
				new_list.append([dc, str(round((dc_found_list[dc]*100)/total_adr_matched,1))+" %"])
			new_list = sorted(new_list,key=itemgetter(1), reverse=True)
			new_list = str(new_list).replace("],","};").replace("[[","{").replace("]]","}").replace("[","{")
			row['DC_Found'] = new_list
			if "_D (" in row['cn'] or "_K (" in row['cn']:
				if row['cn'].lower() not in str(dc_found_list.keys()).lower():
					if "_D (" in row['cn']:
						if row['cn'].replace("_D (", "_K (").lower() not in str(dc_found_list.keys()).lower():
							row['Misroute?'] = 'Yes'
						elif dc_found_list[row['cn'].replace("_D (", "_K (")] == max(dc_found_list.values()):
							row['Misroute?'] = 'No'
						else:
							row['Misroute?'] = 'Maybe'
					elif "_K (" in row['cn']:
						if row['cn'].replace("_K (", "_D (").lower() not in str(dc_found_list.keys()).lower():
							row['Misroute?'] = 'Yes'
						elif dc_found_list[row['cn'].replace("_K (", "_D (")] == max(dc_found_list.values()):
							row['Misroute?'] = 'No'
						else:
							row['Misroute?'] = 'Maybe'
				else:
					if row['cn'].lower() not in str(dc_found_list.keys()).lower():
						row['Misroute?'] = 'Yes'
					elif dc_found_list[row['cn']] == max(dc_found_list.values()):
						row['Misroute?'] = 'No'
					else:
						row['Misroute?'] = 'Maybe'
			elif row['cn'].lower() not in str(dc_found_list.keys()).lower():
				row['Misroute?'] = 'Yes'
			elif dc_found_list[row['cn']] == max(dc_found_list.values()):
				row['Misroute?'] = 'No'
			else:
				row['Misroute?'] = 'Maybe'

		list_to_write.append(row)

	# Printing the final statistics
		print "\r\rFinding document: [%s / %s] [%s %%]" % (i, (len(data_to_search)), (round((i * 100) / len(data_to_search), 1))),

	# Writing the results to a csv file
	status = False
	while status is False:
		try:
			status = writeToCSV(filename, list_to_write)
		except IOError as e:
			ch = raw_input("\nClose the file <" + filename + "> and press any key to continue: ")


def findMongoDocument(phone):
	cursor = col.find({'phone_number': str(phone)})
	document = {}
	for doc in cursor:
		document = doc
	# client.close()
	return document


def csvToDict(filename, my_dict):
	data = open(filename, 'rU')
	reader = csv.reader(data)
	keys = reader.next()
	for row in reader:
		record_dict = dict(zip(keys, row))
		# Store the DC names and the status in lower-case
		my_dict[str(record_dict['DC']).lower()] = str(record_dict['Status']).lower()
	data.close()


if __name__ == '__main__':
	contents_to_search_from, dc_status = [], {}
	readCustomerDataFromCSV("C:\\Users\\Delhivery\\Downloads\\pickupdata_24oct16\\pickupdata_24oct16.csv",
			contents_to_search_from)
	csvToDict("DC_Status.csv", dc_status)
	# Find the document and fetch relevant columns
	findMatch(contents_to_search_from, "result_file_1.csv", dc_status)
	# End of Program
	message = "Program Complete"
	print "\n" + "-" * len(message) + "\n" + message + "\n" + "-" * len(message)
	client.close()
