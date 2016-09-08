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


def findMatch(data_to_search, data_to_search_from, filename):
	list_to_write = []
	for row in data_to_search:
		ratio = 0
		for address in data_to_search_from:
			if fuzz.token_set_ratio(row, address) > ratio :
				ratio = fuzz.token_set_ratio(row, address)
				found_adr = address
		# result = list(process.extractOne(row, data_to_search_from))
		my_dict = {'what_we_searched': row, 'what_we_got': found_adr, 'confidence': ratio}
		list_to_write.append(my_dict)
	status = False
	while status is False:
		try:
			status = writeToCSV(filename, list_to_write)
		except IOError as e:
			ch = raw_input("Close the file <" + filename + "> and press any key to continue: ")


if __name__ == '__main__':
	contents_to_search, contents_to_search_from = [], []
	readCustomerDataFromCSV('sample_file.csv', contents_to_search_from)
	data_to_search_from = []
	for row in contents_to_search_from:
		data_to_search_from.append(row['address_sample_reference'])
	readCustomerDataFromCSV('data_to_search.csv', contents_to_search)
	data_to_search = []
	for row in contents_to_search:
		data_to_search.append(row['address_test'])
	findMatch(data_to_search, data_to_search_from, "found.csv")
	message = "Program Complete"
	print "-" * len(message) + "\n" + message + "\n" + "-" * len(message)
