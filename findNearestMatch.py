from pymongo import MongoClient
import csv
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


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
		records_list.append(record_dict)
	data.close()


def writeToCSV(filename, contents_to_write):
	# PURPOSE:
	#       This function reads the contents of a List of Dictionaries,
	#       and stores them in a csv file.
	with open(filename, 'wb') as f:
		w = csv.DictWriter(f, contents_to_write[0].keys())
		w.writeheader()
		for row in contents_to_write:
			w.writerow(row)
	return True


def findMatch(data_to_search, filename):
	list_to_write = []
	i = 0
	index = 0
	for row in data_to_search:
		print "\r\rFinding document ... [%s / %s]" % (i+1, len(data_to_search)),
		mongoDoc = findMongoDocument(row['ph'])
		max_ratio = 0
		for order in mongoDoc['shipment_details']:
			if fuzz.token_set_ratio(row['add'], order['add']) > max_ratio:
				max_ratio = fuzz.token_set_ratio(row['add'], order['add'])
				index = mongoDoc['shipment_details'].index(order)
		if max_ratio >= 75:
			my_dict = mongoDoc['shipment_details'][index]
			row['dc'] = my_dict['cs_sl']
			row['confidence'] = max_ratio
		else:
			row['dc'] = ''
			row['confidence'] = ''
		list_to_write.append(row)
		i += 1
	# Write the rows to a csv
	status = False
	while status is False:
		try:
			status = writeToCSV(filename, list_to_write)
		except IOError as e:
			ch = raw_input("Close the file <" + filename + "> and press any key to continue: ")


def findMongoDocument(phone):
	client = MongoClient()
	my_db = client.custdb
	col = my_db.cust_details
	cursor = col.find({'phone_number': str(phone)})
	document = {}
	for doc in cursor:
		document = doc
	client.close()
	return document


if __name__ == '__main__':
	contents_to_search_from = []
	readCustomerDataFromCSV('sample_file.csv', contents_to_search_from)
	# Find the document and fetch relevant columns
	findMatch(contents_to_search_from, "sample_file_result.csv")
	# End of Program
	message = "Program Complete"
	print "\n" + "-" * len(message) + "\n" + message + "\n" + "-" * len(message)
