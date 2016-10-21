import csv
import pprint
import os
import glob
import split_csv


def get_file_names():
	dir_path = 'C:\\Users\\Delhivery\\Documents\\GitHub\\misroute_assistant\\New folder'
	songs_list = []
	os.chdir(dir_path)
	for file_name in glob.glob("*.mp3"):
		songs_list.append(file_name)
	return songs_list


def readCustomerDataFromCSV(filename, records_list):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	# ARGUMENTS:
	#       "filename": This is the name of the file from where data has to read
	#       "records_list": This is an empty list into which newly read data will be added
	# Its better to call the records_list by reference than to return it
	data = open(filename, 'rU')
	reader = csv.reader(data)
	keys = reader.next()[1:]
	for row in reader:
		record_dict = dict(zip(keys, row[1:]))
		records_list[row[0]] = record_dict
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


if __name__ == '__main__':
	ref_dict = {}
	readCustomerDataFromCSV("C:\\Users\\Delhivery\\Downloads\\tuscnc.csv",ref_dict)
	# pprint.pprint(ref_dict)
