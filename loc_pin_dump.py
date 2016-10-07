import pprint

from pymongo import MongoClient

client = MongoClient()
mySourceDB = client.all_delivered
SourceColl = mySourceDB.allDeliverData
SourceCursor = SourceColl.find()
DestDB = client.custdb
DestColl = DestDB.loc_pin


def insertDocumentInMongo(document):
	DestColl.insert_one(document)
	return True


def replaceDocument(loc_pin_combo, new_document):
	# This function updates the existing mongo document with the
	# new document with newly added shipment details
	result = DestColl.replace_one({'loc_pin': str(loc_pin_combo)}, new_document)


def fetchMongoDoc(loc_pin_combo):
	cursor = DestColl.find({'loc_pin': loc_pin_combo})
	document = {}
	for doc in cursor:
		document = doc
	return document


if __name__ == '__main__':
	i = 0
	for doc in SourceCursor:
		i = i + 1
		print "\rWorking [%s]" % i,
		loc_pin = str(doc['aseg']['loc']).lower() + str(doc['pin'])
		# If loc_pin_combo already exists in mongodb
		existingDoc = fetchMongoDoc(loc_pin)
		if existingDoc:
			wbnFound = False
			for shipment in existingDoc['shipments']:
				if doc['_id'] == shipment['wbn']:
					wbnFound = True
					break
			if not wbnFound:
				my_dict = {
					'wbn': doc['_id']
				}
				for key in doc:
					if key == '_id':
						continue
					my_dict[key] = doc[key]
				existingDoc['shipments'].append(my_dict)
				replaceDocument(loc_pin, existingDoc)

		# If the loc_pin_combo does not exist in the mongodb
		else:
			my_dict = {
				'wbn': doc['_id']
			}
			for key in doc:
				if key == '_id':
					continue
				my_dict[key] = doc[key]
			whole_doc = {
				'loc_pin': loc_pin,
				'shipments': [my_dict]
			}
			insertDocumentInMongo(whole_doc)
	client.close()
