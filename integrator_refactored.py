#!/usr/bin/env python3

import petl as etl
from pathlib import Path
from collections import OrderedDict

def importUsers():
	try:
		filePath = Path('data/users.csv')
		resolved = filePath.resolve()
	except FileNotFoundError:
		print ("File doesn't exist")
	except Exception as e:
		print("Something went wrong. Error {0}".format(e))
	else:
		return etl.fromcsv(filePath)

def importEvents():
	try:
		filePath = Path('data/events.json')
		resolved = filePath.resolve()
	except FileNotFoundError:
		print("File [{0}] doesn't exist".format(filePath))
	except ValueError:
		print("File [{0}] has errors".format(filePath))
	except Exception as e:
		print("Something went wrong. Error {0}".format(e))
	else:
		return etl.fromjson(filePath)

def createDimCustomers(users):
	try:
		dim_customers = etl.cut(users, 'user_id', 'email')
		etl.tocsv(dim_customers, 'load/dim_customers.csv')
#todo: test if file was created	
	except Exception as e:
	ÂÂ¶¶	print("Something went wrong. Error {0}".format(e))

def createDimSubscriptions(events)
	try:
		dim_subscriptions_cut = etl.cut(events, 'type')
		dim_subscriptions_rename = etl.rename(dim_subscriptions_cut, {'type':'subscription_name'})
		dim_subscriptions = etl.distinct(dim_subscriptions_rename)
		# Export as csv to load folder
		etl.tocsv(dim_subscriptions, 'load/dim_subscriptions.csv')
#todo: test if file was created	
	except Exception as e:
	ÂÂ¶¶	print("Something went wrong. Error {0}".format(e))


def createDimMedium(events):
	try:
		dim_subscriptions_cut = etl.cut(events, 'type')
		dim_subscriptions_rename = etl.rename(dim_subscriptions_cut, {'type':'subscription_name'})
		dim_subscriptions = etl.distinct(dim_subscriptions_rename)
		# Export as csv to load folder
		etl.tocsv(dim_subscriptions, 'load/dim_subscriptions.csv')
	except Exception as e:
		print("Something went wrong. Error {0}".format(e))


# Note: 
#       Slowly changing dimension
#       No data for now, meaning that until we have this defined we will fill everything with a "none"
#       Let's define that this will be solved until the end of september, and the start date was on the 28 of April 2018
def createDimSource():
	try:
		tbl_campaign = [['campaign_name', 'campaign_started', 'campaign_ended'], ['none', '2014-04-28T00:00:00', '2018-09-30T00:00:00']]
		dim_campaign = etl.head(tbl_campaign, 1)
		# Export as csv to load folder
		etl.tocsv(dim_campaign, 'data/campaign.csv')
	except Exception as e:
		print("Something went wrong. Error {0}".format(e))

# DimTime
# This should be managed on the database side


# This facts table will be the staging with all the needed info to quickly update with the dimension keys and load to the facts table
# The facts table will have columns to match each column on the dim Time table, to make it easier to get the reference key
def createFacts(events, users):
	try:
		events_uid = etl.cutout(events, 'tracking_id', 'utm_medium', 'utm_campaign')
		events_tui = etl.cutout(events, 'user_id')

		stage_uid = etl.join(users, events_uid, key='user_id')
		stage_tui = etl.join(users, events_tui, key='tracking_id')

		stage_utm = etl.cut(stage_tui, 'user_id', 'utm_medium', 'utm_campaign')
		stage_uid_utm = etl.join(stage_uid, stage_utm, key='user_id')
		stage_m_s = etl.mergesort(stage_uid_utm, stage_tui, key=['created_at', 'email'])

		mappings = OrderedDict()
		mappings['tid']='tracking_id'
		mappings['uid']= 'user_id'
		mappings['utm_medium']='utm_medium'
		mappings['utm_campaign']='utm_campaign'
		mappings['email']='email'
		mappings['subscription']='type'
		mappings['sub_order']='type', {'Signup Completed': '1', 'Trial Started':'2', 'Subscription Started':'3', 'Subscription Ended':'4'}
		mappings['created_at']='created_at'

		# Mapping
		stage_mapping = etl.fieldmap(stage_m_s, mappings)

		# Sort
		stage_mapping_ordered = etl.sort(stage_mapping, key=['created_at', 'email', 'sub_order'])

		# Datetime split
		t1 = etl.split(stage_mapping_ordered, 'created_at', 'T', ['date', 'time'], include_original=True)
		t2 = etl.split(t1, 'date', '-', ['year', 'month', 'day'])
		stage_ready = etl.split(t2, 'time', ':', ['hour', 'minute', 'second'])

		# Export as csv Ãto load folder
		etl.tocsv(stage_ready, 'load/facts.csv')

	except Exception as e:
		print("Something went wrong. Error {0}".format(e))


def main():
	'''main function
	'''
	users = importUsers()
	#test
	try:
		print(users.look())
	except Exception as e:
		print("Something went wrong. Error: {0}".format(e))
	events = importEvents()
	#test
	try:
		print(events.look())
	except Exception as e:
		print("Something went wrong. Error: {0}".format(e))
		


if __name__ == '__main__':
	 main()
