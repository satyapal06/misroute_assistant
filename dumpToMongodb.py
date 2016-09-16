import pprint
from pymongo import MongoClient
import csv
from Source import formatting


client = MongoClient()
my_db = client.custdb
col = my_db.cust_details_new


def readCustomerDataFromCSVandFormat(filename):
	# PURPOSE:
	#       This function reads the contents of customer data from the CSV file
	#       formats the phone numbers and the addresses.
	#       For each row that is read, its formatted first and then passed into
	#       the function 'main_body'. All these steps happen row by row.
	# ARGUMENTS:
	#       "filename": This is the name of the file from where data has to read
	print "Determining file size ... ",
	total_row_count = 0
	with open(filename, "r") as f:
		reader = csv.reader(f, delimiter=",")
		data = list(reader)
		total_row_count = len(data)

	data = open(filename, 'rU')
	reader = csv.reader(data)
	# total_row_count = len(list(reader))
	keys = reader.next()
	i = 1
	for row in reader:
		print "\r\rWorking on Row [%s/%s]" % (i, total_row_count),
		record_dict = dict(zip(keys, row))
		# We only need those records where status is 'Delivered'
		if record_dict['cs.st'] in ['DL', 'dl']:
			# Formatting the phone and the address
			record_dict['ph'] = formatting.formatPhone(record_dict['ph'])
			record_dict['add'] = formatting.formatAddress(record_dict['add'])
			# Sending this row_of_data to mongodb
			main_body(record_dict)
			i += 1
	data.close()


def insertDocumentInMongo(document):
	# PURPOSE:
	#       This function accepts the documents that were created by the function
	#       'collectDataForEachPhone' and stores them in mongodb
	# ARGUMENTS:
	#       "all_documents": This is a list containing the JSON style documents
	#       to be stored in mongodb
	# client = MongoClient()
	# my_db = client.custdb
	# col = my_db.cust_details_new
	col.insert_one(document)
	# client.close()


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
	# client = MongoClient()
	# db = client.custdb
	# col = db.cust_details_new
	result = col.replace_one({'phone_number': str(phone)}, new_document)
	# client.close()


def fetchMongoDoc(phone):
	# client = MongoClient()
	# my_db = client.custdb
	# col = my_db.cust_details_new
	cursor = col.find({'phone_number': str(phone)})
	document = {}
	for doc in cursor:
		document = doc
	# client.close()
	return document


def main_body(row_of_data):
	for phone in row_of_data['ph']:
		# For each phone in the ph column of the row, check if that
		# phone is present in mongodb
		doc = fetchMongoDoc(phone)

		#  If the phone already exists in mongodb
		if doc:
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
				new_order = {
					"wbn": row_of_data['wbn'],
					"em": row_of_data["em"],
					"nsl": row_of_data["nsl"],
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
				# Code to replace the existing doc in mongo
				replaceDocument(phone, doc)

		# If the phone doesn't exist in mongodb
		else:
			# Create the document
			document_to_insert = {
				'phone_number': phone,
				'shipment_details': [
					{
						"wbn": row_of_data['wbn'],
						"em": row_of_data["em"],
						"nsl": row_of_data["nsl"],
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
				]
			}
			# Insert the document into mongodb
			insertDocumentInMongo(document_to_insert)


if __name__ == '__main__':
	readCustomerDataFromCSVandFormat("till_13303787.csv")
	client.close()