#!/usr/bin/env python3
import petl as etl
from collections import OrderedDict

# Load the files
users = etl.fromcsv('data/users.csv')
events = etl.fromjson('data/events2.json')

# Transform
# Dim Customers
# Filter necessary data only
dim_customers = etl.cut(users, 'user_id', 'email')
# Export as csv to load folder
etl.tocsv(dim_customers, 'load/dim_customers.csv')


# Dim Subscriptions
# Use the distinct values present in the type column to load  into the dim subscription table
dim_subscriptions_cut = etl.cut(events, 'type')
dim_subscriptions_rename = etl.rename(dim_subscriptions_cut, {'type':'subscription_name'})
dim_subscriptions = etl.distinct(dim_subscriptions_rename)
# Export as csv to load folder
etl.tocsv(dim_subscriptions, 'load/dim_subscriptions.csv')


# Dim Medium
# Use the distinct values present in the utm_medium colum to load into the dim medium table
dim_medium_cut = etl.cut(events, 'utm_medium')
dim_medium_rename = etl.rename(dim_medium_cut, {'utm_medium': 'medium_name'})
dim_medium = etl.distinct(dim_medium_rename)
# Export as cvs to load folder
etl.tocsv(dim_medium, 'load/dim_medium.csv')


# Dim Source
# Use the distinct values present in the utm_campaign column to load into the dim campaign table
# Note: 
#	If this is the only available data right now, this in the future will probably be the source and not campaign
#	Another table, with the campaign name and start and end date will be connect with the facts table
dim_source_cut = etl.cut(events, 'utm_campaign')
dim_source_rename = etl.rename(dim_source_cut, {'utm_campaign': 'source_name'})
dim_source = etl.distinct(dim_source_rename)
#export as csv to load folder
etl.tocsv(dim_source, 'load/dim_source.csv')


# Dim Campaign 
# Note: 
#	Slowly changing dimension
# 	No data for now, meaning that until we have this defined we will fill everything with a "none"
#	Let's define that this will be solved until the end of september, and the start date was on the 28 of April 2018
tbl_campaign = [['campaign_name', 'campaign_started', 'campaign_ended'], ['none', '2014-04-28T00:00:00', '2018-09-30T00:00:00']]
dim_campaign = etl.head(tbl_campaign, 1)
# Export as csv to load folder
etl.tocsv(dim_campaign, 'data/campaign.csv')



# Dim Time
# TO DO
#	Load a full year (2018) with the most simple datetime analysis
#	Year, month, day, hour, minute, second

#	For the full loading process , use the reference on the references.txt
#	This should be a processure with all the validation logic there, to create the next X months when it is called


#  Facts

# This facts table will be the staging with all the needed info to quickly update with the dimension keys and load to the facts table
# The facts table will have columns to match each column on the dim Time table, to make it easier to get the reference key
#

events_uid = etl.cutout(events, 'tracking_id', 'utm_medium', 'utm_campaign')
events_tui = etl.cutout(events, 'user_id')

stage_uid = etl.join(users, events_uid, key='user_id')
stage_tui = etl.join(users, events_tui, key='tracking_id')
stage_utm = etl.cut(stage_tui, 'user_id', 'utm_medium', 'utm_campaign')
stage_uid_utm = etl.join(stage_uid, stage_utm, key='user_id')
stage_m_s = etl.mergesort(stage_uid_utm, stage_tui, key=['created_at', 'email'])

# Mapping definitions
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

# Export as csv to load folder
etl.tocsv(stage_ready, 'load/facts.csv')
