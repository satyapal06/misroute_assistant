import pprint
from pymongo import MongoClient
import csv
from Source import formatting


client = MongoClient()
my_db = client.custdb
col = my_db.cust_details


def mycsv_reader(csv_reader):
	while True:
		try:
			yield next(csv_reader)
		except csv.Error:
			# error handling what you want.
			pass
		continue
	return


def readCustomerDataFromCSVandFormat(filename):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	#       formats the phone numbers and the addresses.
	#       For each row that is read, its formatted first and then passed into
	#       the function 'main_body'. All these steps happen row by row.
	# ARGUMENTS:
	#       "filename": This is the name of the file from where data has to read

	# print "Determining file size ... ",
	# total_row_count = 0
	# with open(filename, "r") as f:
	# 	reader = csv.reader(f, delimiter=",")
	# 	data = list(reader)
	# 	# The total rows includes the header row also
	# 	total_row_count = len(data) - 1

	# To resolve the null byte error, used the solution recommended here:
	# http://stackoverflow.com/questions/26050968/line-contains-null-byte-error-in-python-csv-reader
	reader = mycsv_reader(csv.reader(open(filename, 'rU')))
	# total_row_count = len(list(reader))
	keys = reader.next()
	i = 0
	for row in reader:
		try:
			i += 1
			print "\r\rWorking on Row [%s]" % (i),
			if i < 4184932:
				continue
			record_dict = dict(zip(keys, row))
			# We only need those records where status is 'Delivered'
			if record_dict['cs.st'] in ['DL', 'dl']:
				# Formatting the phone and the address
				record_dict['ph'] = formatting.formatPhone(record_dict['ph'])
				record_dict['add'] = formatting.formatAddress(record_dict['add'])
				# Sending this row_of_data to mongodb
				main_body(record_dict)
		except csv.Error as e:
			continue
	# data.close()


def insertDocumentInMongo(document):
	# PURPOSE:
	#       This function accepts the documents that were created by the function
	#       'collectDataForEachPhone' and stores them in mongodb
	# ARGUMENTS:
	#       "all_documents": This is a list containing the JSON style documents
	#       to be stored in mongodb
	col.insert_one(document)
	return True


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


def replaceDocument(phone, new_document):
	# This function updates the existing mongo document with the
	# new document with newly added shipment details
	result = col.replace_one({'phone_number': str(phone)}, new_document)


def fetchMongoDoc(phone):
	cursor = col.find({'phone_number': str(phone)})
	document = {}
	for doc in cursor:
		document = doc
	return document


def main_body(row_of_data):
	for phone in row_of_data['ph']:
		# For each phone in the ph column of the row, check if this
		# particular phone number is present in mongodb
		doc = fetchMongoDoc(phone)
		#  If the phone already exists in mongodb
		if doc:
			need_to_replace_document = False
			#  Check if the current wbn is already present in the order history
			already_present = False
			for order in doc['shipment_details']:
				if row_of_data['wbn'] == order['wbn']:
					already_present = True
					break
			# If the wbn is already present
			if already_present:
				# Do nothing
				pass
			# If the wbn is not present, then add the order to existing document
			else:
				need_to_replace_document = True
				new_order = {
					"wbn": row_of_data['wbn'],
					"em": row_of_data["em"],
					# "nsl": row_of_data["nsl"],
					"nsl": '',  # Modified on 26th October
					# Reason: NSL code not recieved, so inserting blanks in mongodb
					"cn": row_of_data["cn"],
					"pin": row_of_data["pin"],
					"cl": row_of_data["cl"],
					"fpd": row_of_data["fpd"],
					"nm": row_of_data["nm"],
					"pt": row_of_data["pt"],
					"add": row_of_data["add"],
					"rcn": row_of_data["rcn"],
					"aseg_loc": row_of_data["aseg.loc"],
					"cnc": row_of_data["cnc"],
					"pd": row_of_data["pd"],
					"cs_st": row_of_data["cs.st"],
					"cpin": row_of_data["cpin"],
					"cs_ss": row_of_data["cs.ss"],
					"cs_sl": row_of_data["cs.sl"],
					"cs_sd": row_of_data["cs.sd"],
					"aseg_pin": row_of_data["aseg.pin"],
					"cty": row_of_data["cty"],
					"pdd": row_of_data["pdd"]
				}
				doc['shipment_details'].append(new_order)
			# Update alternate numbers
			alternate_no = []
			for num in row_of_data['ph']:
				if num == phone:
					pass
				else:
					alternate_no.append(num)
			if alternate_no:
				for num in alternate_no:
					if num not in doc['alternate_no']:
						need_to_replace_document = True
						doc['alternate_no'].append(num)
			# If the document needs to be replaced
			if need_to_replace_document:
				# Code to replace the existing doc in mongo
				replaceDocument(phone, doc)

		# If the phone doesn't exist in mongodb
		else:
			# Find the alternate numbers
			alternate_no = []
			for num in row_of_data['ph']:
				if num == phone:
					pass
				else:
					alternate_no.append(num)
			# Create the document
			document_to_insert = {
				'phone_number': phone,
				'shipment_details': [
					{
						"wbn": row_of_data['wbn'],
						"em": row_of_data["em"],
						# "nsl": row_of_data["nsl"],
						"nsl": '',  # Modified on 26th October
						# Reason: NSL code not recieved, so inserting blanks in mongodb
						"cn": row_of_data["cn"],
						"pin": row_of_data["pin"],
						"cl": row_of_data["cl"],
						"fpd": row_of_data["fpd"],
						"nm": row_of_data["nm"],
						"pt": row_of_data["pt"],
						"add": row_of_data["add"],
						"rcn": row_of_data["rcn"],
						"aseg_loc": row_of_data["aseg.loc"],
						"cnc": row_of_data["cnc"],
						"pd": row_of_data["pd"],
						"cs_st": row_of_data["cs.st"],
						"cpin": row_of_data["cpin"],
						"cs_ss": row_of_data["cs.ss"],
						"cs_sl": row_of_data["cs.sl"],
						"cs_sd": row_of_data["cs.sd"],
						"aseg_pin": row_of_data["aseg.pin"],
						"cty": row_of_data["cty"],
						"pdd": row_of_data["pdd"]
					}
				],
				'alternate_no': alternate_no
			}
			# Insert the document into mongodb
			status = insertDocumentInMongo(document_to_insert)


if __name__ == '__main__':
	readCustomerDataFromCSVandFormat("D:\\pickupdata_1sep_25oct16 (1)\\pickupdata_1sep_25oct16.csv")
	client.close()