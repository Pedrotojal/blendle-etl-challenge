#!/usr/bin/env python3

import petl as etl
import datetime
import sys
import time

#Load the files
users = etl.fromcsv('data/users.csv')
events = etl.fromjson('data/events2.json')

##########
#Transform
#########

############
##DIMENSIONS


#Dim Customers
#Filter necessary data only
dim_customers = etl.cut(users, 'user_id','email')
#export as csv to load folder
etl.tocsv(dim_customers,'load/dim_customers.csv')


#Dim Subscriptions
#Use the distinct values present in the type column to load  into the dim subscription table
dim_subscriptions_cut = etl.cut(events, 'type')
dim_subscriptions_rename = etl.rename(dim_subscriptions_cut, {'type':'subscription_name'})
dim_subscriptions = etl.distinct(dim_subscriptions_rename)
#export as csv to load folder
etl.tocsv(dim_subscriptions, 'load/dim_subscriptions.csv')


#Dim Medium
#Use the distinct values present in the utm_medium colum to load into the dim medium table
dim_medium_cut = etl.cut(events, 'utm_medium')
dim_medium_rename = etl.rename(dim_medium_cut, {'utm_medium':'medium_name'})
dim_medium = etl.distinct(dim_medium_rename)
#export as cvs to load folder
etl.tocsv(dim_medium, 'load/dim_medium.csv')


#Dim Source
#Use the distinct values present in the utm_campaign column to load into the dim campaign table
#Note: 
#	If this is the only available data right now, this in the future will probably be the source and not campaign
#	Another table, with the campaign name and start and end date will be connect with the facts table
dim_source_cut = etl.cut(events,'utm_campaign')
dim_source_rename = etl.rename(dim_source_cut, {'utm_campaign':'source_name'})
dim_source = etl.distinct(dim_source_rename)
#export as csv to load folder
etl.tocsv(dim_source, 'load/dim_source.csv')


#Dim Campaign 
#Note: 
#	Slowly changing dimension
# 	No data for now, meaning that until we have this defined we will fill everything with a "none"
#	Let's define that this will be solved until the end of september, and the start date was on the 28 of April 2018
tbl_campaign = [['campaign_name','campaign_started', 'campaign_ended'],['none','2014-04-28T00:00:00','2018-09-30T00:00:00']]
dim_campaign = etl.head(tbl_campaign, 1)
#export as csv to load folder
etl.tocsv(dim_campaign, 'data/campaign.csv')



#Dim Time
# TO DO
#	Load a full year (2018) with the most simple datetime analysis
#	Year, month, day, hour, minute, second

#	For the full loading process , use the reference on the references.txt
#	This should be a processure with all the validation logic there, to create the next X months when it is called


######
#FACTS

# This facts table will be the staging with all the needed info to quickly update with the dimension keys and load to the facts table
# The facts table will have columns to match each column on the dim Time table, to make it easier to get the reference key
#

