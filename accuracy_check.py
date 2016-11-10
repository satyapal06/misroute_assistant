import os, sys, glob
import csv


def readCustomerDataFromCSVnew(filename, records_list):
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
		if record_dict['Misroute?'] in ["Yes", "yes"]:
			records_list.append(record_dict['wbn'])
	data.close()


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
		records_list.append(record_dict['wbn'])
	data.close()


def writeToCSV(filename, contents_to_write):
	# PURPOSE:
	#       This function reads the contents of a List of Dictionaries,
	#       and stores them in a csv file.
	myKeys = ['wbn', 'prediction_status']
	with open(filename, 'wb') as f:
		w = csv.DictWriter(f, myKeys)
		w.writeheader()
		for row in contents_to_write:
			w.writerow(row)
	return True


def get_file_names():
	dir_path = "C:\\Users\\Delhivery\\Downloads\\Daily Misroute Data"
	file_list = []
	os.chdir(dir_path)
	for file_name in glob.glob("*.csv"):
		file_list.append(file_name)
	return file_list


if __name__ == '__main__':
	misrouted_wbn_list = []
	all_files = get_file_names()
	print "Reading all files"
	for each_file in all_files:
		readCustomerDataFromCSV(each_file,misrouted_wbn_list)
		misrouted_wbn_list = list(set(misrouted_wbn_list))

	print "Reading wbn from report"
	predicted_wbn_misroute = []
	readCustomerDataFromCSVnew("C:\\Users\\Delhivery\\Downloads\\results\\results\\result_file_1.csv", predicted_wbn_misroute)

	print "Finding Accuracy"
	dict_to_write = []
	correct_prediction = 0
	wrong_prediction = 0
	total_wbn = len(predicted_wbn_misroute)
	i = 1
	for wbn in predicted_wbn_misroute:
		print "\rCount %s" % i,
		if wbn in misrouted_wbn_list:
			correct_prediction += 1
			dict_to_write.append({'wbn': wbn, 'prediction_status': 'correct'})
		else:
			wrong_prediction += 1
			dict_to_write.append({'wbn': wbn, 'prediction_status': 'wrong'})
		i += 1

	writeToCSV("prediction_accuracy.csv", dict_to_write)
	print "\n\nTotal : %s" % total_wbn
	print "Correct Prediction count : %s" % correct_prediction
	print "Percentage : %s" % round(((correct_prediction*100)/total_wbn), 2)