import csv
from pymongo import MongoClient
import formatting


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


def organizeData(original_contents, new_contents):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file,
	#       formats the phone numbers, addresses and then splits the rows
	#       if they contain multiple phone numbers
	# ARGUMENTS:
	#       "original_contents" is the data read from the csv file
	#       "new_contents" is a list where the split data is stored (Call by Reference)
	current_item = 1
	for row in original_contents:
		print "\rOrganising Data ... [%s / %s]" % (current_item, len(original_contents)),
		new_adr = formatting.formatAddress(row['add'])
		row['add'] = formatting.mergeAbbreviations(new_adr)
		phone_numbers = formatting.formatPhone(row['ph'])
		if len(phone_numbers) > 1:
			for phone in phone_numbers:
				row['ph'] = phone
				new_contents.append(row)
		elif len(phone_numbers) == 1:
			row['ph'] = "".join(phone_numbers)
			new_contents.append(row)
		else:
			pass  # Don't add those rows
		current_item += 1


def findUniquePhoneNumbers(contents):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	#       and finds all the unique phone numbers
	# ARGUMENTS:
	#       "contents" is the split data
	phone_number_list = []
	for row in contents:
		phone_number_list.append(row['ph'])
	unique_phone_number_list = list(set(phone_number_list))
	return unique_phone_number_list


def collectDataForEachPhone(unique_phone_numbers, contents):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	#       and create JSON style documents for each unique phone number.
	#       A document for a phone number will contain the list of all the WBNs,
	#       all the previous addresses, the corresponding DCc and so forth.
	# ARGUMENTS:
	#       "unique_phone_numbers": Its a list containing all the unique mobiles
	#       "contents": this the split content
	#  RETURNS:
	#       The function returns all the documents for all the phone numbers
	list_of_documents_created = []
	current_item = 1
	for phone_number in unique_phone_numbers:
		print "\rCreating JSON and storing mongo documents ... [%s / %s]" % (current_item, len(unique_phone_numbers)),
		cust_name = []
		cust_email = []
		shipment_details = []
		add_dc_history = []
		for row in contents:
			if phone_number == row['ph']:
				cust_name.append(row['nm'])
				if cust_email: cust_email.append(row['em'])
				shipment_details.append(
					{
						"wbn": row['wbn'],
						"nsl": row["nsl"],
						"cn": row["cn"],
						"pin": row["pin"],
						"cl": row["cl"],
						"fpd": row["fpd"],
						"nm": row["nm"],
						"pt": row["pt"],
						"occ": row["occ"],
						"add": row["add"],
						"rcn": row["rcn"],
						"aseg_loc": row["aseg.loc"],
						"cnc": row["cnc"],
						"pd": row["pd"],
						"cs_st": row["cs.st"],
						"cpin": row["cpin"],
						"cs_ss": row["cs.ss"],
						"cs_sl": row["cs.sl"],
						"cs_sd": row["cs.sd"],
						"aseg_pin": row["aseg.pin"],
						"cty": row["cty"],
						"pdd": row["pdd"]
					}
				)
		myDict = {
			"phone_number": phone_number,
			"customer_name": list(set(cust_name)),
			"customer_email": list(set(cust_email)),
			"shipment_details": shipment_details
		}
		# list_of_documents_created.append(myDict)
		updateMongo(myDict)
		current_item += 1
	# return list_of_documents_created


def updateMongo(all_documents):
	# PURPOSE:
	#       This function accepts the documents that were created by the function
	#       'collectDataForEachPhone' and stores them in mongodb
	# ARGUMENTS:
	#       "all_documents": This is a list containing the JSON style documents
	#       to be stored in mongodb
	client = MongoClient()
	my_db = client.custdb
	col = my_db.cust_details
	col.insert_one(all_documents)
	client.close()


def writeToCSV(contents):
	# PURPOSE:
	#       This function reads the contents of a List of Dictionaries, and stores
	#       them in a csv file.
	with open('mycsvfile.csv', 'wb') as f:
		w = csv.DictWriter(f, contents[0].keys())
		w.writeheader()
		for row in contents:
			w.writerow(row)


if __name__ == '__main__':
	csv_contents, split_contents = [], []
	print "Reading file ...",
	readCustomerDataFromCSV("city_file.csv", csv_contents)
	print "\r<Complete> Reading file ..."
	print "Organizing data ...",
	organizeData(csv_contents, split_contents)
	print "\r<Complete> Organizing data ..."
	# writeToCSV(split_contents)
	print "Finding unique phone numbers ...",
	unique_numbers = findUniquePhoneNumbers(split_contents)
	print "\r<Complete> Finding unique phone numbers ..."
	print "Creating mongo documents ...",
	collectDataForEachPhone(unique_numbers, split_contents)
	print "\r\r<Complete> Creating JSON documents ..."
	print "\r\r<Complete> Pushing documents to mongodb ..."
	msg = "***** End of Program *****"
	print ("-" * len(msg)) + "\n" + msg + ("\n" + "-" * len(msg))