import csv


def mycsv_reader(csv_reader):
	while True:
		try:
			yield next(csv_reader)
		except csv.Error:
			# error handling what you want.
			pass
		continue
	return


def get_csv_records(filename, reader, data, keys, iteration):
	if reader is None:
		# data = open(filename, 'rU')
		# reader = csv.reader(data)
		reader = mycsv_reader(csv.reader(open(filename, 'rU')))
		keys = reader.next()
	row_count = 0
	my_list = []

	for row in reader:
		my_dict = dict(zip(keys, row))
		if str(my_dict['cnc']).lower() in ['siliguri', 'jorhat', 'itanagar', 'naharlagun', 'agartala', 'shillong', 'gangtok', 'aizawl', 'jaigaon', 'tinsukia', 'guwahati']:
		# 	# print row
		# 	# raw_input()
			my_list.append(my_dict)
			row_count += 1
		if row_count == 150000:
			print "Part %s Complete." % (iteration)
			writeToCSV(
				"C:\\Users\\Delhivery\\Documents\\GitHub\\misroute_assistant\\New folder\\part_" + str(iteration) + ".csv", my_list)
			my_list = []
			get_csv_records(filename, reader, data, keys, iteration + 1)

	print "Part %s Complete." % (iteration)
	writeToCSV("C:\\Users\\Delhivery\\Documents\\GitHub\\misroute_assistant\\New folder\\part_" + str(
		iteration) + ".csv", my_list)
	# data.close()
	print "Splitting complete !"
	exit()


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
	get_csv_records("D:\\pickupdata_1sep_25oct16 (1)\\pickupdata_1sep_25oct16.csv", None, None, None, 1)