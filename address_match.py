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


def writeToCSV(contents_to_write):
	# PURPOSE:
	#       This function reads the contents of a List of Dictionaries,
	#       and stores them in a csv file.
	with open('mycsvfile.csv', 'wb') as f:
		w = csv.DictWriter(f, contents_to_write[0].keys())
		w.writeheader()
		for row in contents_to_write:
			w.writerow(row)


def fuzzyMatch(reference_contents):
	new_contents_with_confidence = []
	for row in reference_contents:
		row['token_set'] = fuzz.token_set_ratio(row['address_test'].lower(),row['address_sample_reference'].lower())
		row['token_sort'] = fuzz.token_sort_ratio(row['address_test'].lower(), row['address_sample_reference'].lower())
		row['default_fuzzy'] = fuzz.ratio(row['address_test'].lower(), row['address_sample_reference'].lower())
		new_contents_with_confidence.append(row)
	writeToCSV(new_contents_with_confidence)


if __name__ == '__main__':
	contents = []
	readCustomerDataFromCSV("sample_file.csv", contents)
	fuzzyMatch(contents)
	message = "Program Complete"
	print "-" * len(message) + "\n" + message + "\n" + "-" * len(message)