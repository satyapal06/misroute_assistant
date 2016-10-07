import csv
from pymongo import MongoClient

client = MongoClient()
my_db = client.custdb
coll = my_db.loc_pin
cursor = coll.find()


def list_dcs(count):
	i = 0
	list_to_write = []
	for doc in cursor:
		print "\rWorking [%s]" % (i + 1),
		locpincombo = doc['loc_pin']
		dc_list = {}
		for shipment in doc['shipments']:
			dc_list.setdefault(shipment['cn'], 0)
			dc_list[shipment['cn']] += 1
		list_to_write.append(
			{
				'loc_pin': locpincombo,
				'total_shipments' : len(doc['shipments']),
				'dc_list': dc_list,
				'count_of_dc' : len(dc_list)
			}
		)
		i += 1
		if i == count:
			break
	return list_to_write


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
	list_to_write = list_dcs(1000)
	writeToCSV("dc_list.csv", list_to_write)
	client.close()