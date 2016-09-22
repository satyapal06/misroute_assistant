import pprint
import csv
from pymongo import MongoClient

client = MongoClient()
my_db = client.custdb
col = my_db.cust_details
cursor = col.find()

def stats():
	# Iterate over the documents one by one
	overall_stats = []
	i = 0
	for doc in cursor:
		i += 1
		print "\rWorking on document [%s]" % i,
		my_dict = {}
		# Initialize the variables to store stats
		names_list = []
		address_list = []
		my_dict['phone'] = doc['phone_number']
		my_dict['names_count_under_this_phone'] = 0
		my_dict['names_count_under_all_phones'] = 0
		my_dict['alternate_numbers_count'] = 0
		my_dict['order_count_under_current_phone'] = 0
		my_dict['order_count_under_all_phones'] = 0
		my_dict['addresses_count_under_current_phone'] = 0
		my_dict['addresses_count_under_all_phones'] = 0

		for order in doc['shipment_details']:
			my_dict['order_count_under_current_phone'] += 1
			my_dict['order_count_under_all_phones'] += 1

			names_list.append(order['nm'])
			address_list.append(order['add'])

		my_dict['names_count_under_this_phone'] = len(list(set(names_list)))
		my_dict['addresses_count_under_current_phone'] = len(list(set(address_list)))

		if doc['alternate_no']:
			for num in doc['alternate_no']:
				my_dict['alternate_numbers_count'] += 1
				new_cursor = col.find({'phone_number': str(num)})
				alt_doc = {}
				for docs in new_cursor:
					alt_doc = docs
				for orders in alt_doc['shipment_details']:
					my_dict['order_count_under_all_phones'] += 1
					names_list.append(orders['nm'])
					address_list.append(orders['add'])
		my_dict['names_count_under_all_phones'] = len(list(set(names_list)))
		my_dict['addresses_count_under_all_phones'] = len(list(set(address_list)))
		overall_stats.append(my_dict)
	writeToCSV("stats.csv", overall_stats)


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
	stats()
	client.close()
