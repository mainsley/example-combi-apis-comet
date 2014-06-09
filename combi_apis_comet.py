'''
Dependencies: Python 2.7 and import.io Python Client

@author: ignacioelola
'''

import logging
import threading
import json
import ConfigParser
import csv
import sys

import importio, latch

output_name = sys.argv[1] if len(sys.argv) > 1 else raw_input("Name of the output CSV file you want to create: ")

# First of all we need to read the config file:
config = ConfigParser.ConfigParser()
config.read("config_options.ini")
user_id=config.get("combi_apis","user_id")
api_key=config.get("combi_apis","api_key")
extractor_guid_1=config.get("combi_apis","extractor_guid_1")
extractor_guid_2=config.get("combi_apis","extractor_guid_2")
input_first_extractor=config.get("combi_apis","input_first_extractor")
input_second_extractor=config.get("combi_apis","input_second_extractor")
# If the config file has a starting url, the script will use that. If not, it will ask the user for a csv file to load a list of urls.
try:
    starting_query=config.get("combi_apis","starting_query")
except:
    file_with_urls = raw_input("Name of CSV file containing the input urls: ")
    starting_query=[]
    with open(file_with_urls+".csv", "rb") as infile:
        reader=csv.reader(infile)
        for row in reader:
            starting_query.append(row[0])



def callback(query, message):
  global current_results
  
  # Disconnect messages happen if we disconnect the client library while a query is in progress
  if message["type"] == "DISCONNECT":
    print "Query in progress when library disconnected"
    print json.dumps(message["data"], indent = 4)

  # Check the message we receive actually has some data in it
  if message["type"] == "MESSAGE":
    if "errorType" in message["data"]:
      # In this case, we received a message, but it was an error from the external service
      print "Got an error!" 
      print json.dumps(message["data"], indent = 4)
    else:
      # We got a message and it was not an error, so we can process the data
      #print "Got data!"
      #print json.dumps(message["data"], indent = 4)
      # Save the data we got in our current_results variable for later
      current_results[message["data"]["pageUrl"]]=message["data"]["results"]
  
  # When the query is finished, countdown the latch so the program can continue when everything is done
  if query.finished(): queryLatch.countdown()

# Initialise the library
# To use an API key for authentication, use the following code:
client = importio.importio(user_id=user_id, 
  api_key=api_key, 
  host="https://query.import.io")

client.connect()

# Now we are going to query the first extractor
print "Querying the first extractor:"
# If the input for the first extractor is onyl one:
if isinstance(starting_query,list)==False:
    # Use a latch to stop the program from exiting
    queryLatch = latch.latch(1)
    current_results = {}

    # Querying extractor 1:
    client.query({
      "connectorGuids": [
        extractor_guid_1
      ],
      "input": {
        input_first_extractor: starting_query
      }
    }, callback)

    # Wait until queries complete
    queryLatch.await()

    # Here we create a list with all outputs from extractor 1 that we are going to use as inputs in extractor 2.
    inputs_second_extractor=[]
    first_query_results={}
    for key in current_results:
        for result in current_results[key]:
            inputs_second_extractor.append(result[input_second_extractor])
            # We re-organize the first results into a dictionary using second extractor inputs as keys.
            first_query_results[result[input_second_extractor]]=result

# If the input for the first extractor is a list of urls, we iterate the query, making 10 queries at once
else:
    len_last_batch=len(starting_query)%10
   # Use a latch to stop the program from exiting
    queryLatch = latch.latch(10)

    # Querying extractor 1:
    num_queries_in_batch=0
    inputs_second_extractor=[]
    first_query_results={}
    current_results={}
    for input_ in starting_query:
        print "Query #%s of %s " % (starting_query.index(input_), len(starting_query))
        queries_to_made=len(starting_query)-starting_query.index(input_)
        if queries_to_made==len_last_batch:
            queryLatch = latch.latch(len_last_batch)
        client.query({
          "connectorGuids": [
            extractor_guid_1
          ],
          "input": {
            input_first_extractor: input_
          }
        }, callback)
        num_queries_in_batch=num_queries_in_batch+1 
        print queries_to_made
        # Wait until queries complete
        if num_queries_in_batch==10 or queries_to_made==1:
            queryLatch.await()
            num_queries_in_batch=0
            # Here we create a list with all outputs from extractor 1 that we are going to use as inputs in extractor 2.
            for url in current_results:
                for result in current_results[url]:
                    inputs_second_extractor.append(result[input_second_extractor])
                    # We re-organize the first results into a dictionay using second extractor inputs as keys.
                    first_query_results[result[input_second_extractor]]=result

print first_query_results
# Defining new Latch to do ten queries at once for the second extractor:
queryLatch = latch.latch(10)

# Now we query extractor 2, appending the new results in a new dictionary of results to write in the csv
num_queries_in_batch=0
current_results={}
query_results_to_write={}
header=[]
len_last_batch=len(inputs_second_extractor)%10
print "Now the second extractor: "
with open(output_name+".csv","a") as outfile:
    writer=csv.writer(outfile)
    for input_to_query in inputs_second_extractor:
        print "Query #%s of %s " % (inputs_second_extractor.index(input_to_query), len(inputs_second_extractor))
        queries_to_made=len(inputs_second_extractor)-inputs_second_extractor.index(input_to_query)
        if queries_to_made==len_last_batch:
            queryLatch = latch.latch(len_last_batch)
        client.query({
          "connectorGuids": [
            extractor_guid_2
          ],
          "input": {
            "webpage/url": input_to_query
          }
        }, callback)
        num_queries_in_batch=num_queries_in_batch+1

        # We wait for the responses of the batch of queries
        if num_queries_in_batch==10 or queries_to_made==1:
            queryLatch.await()
            num_queries_in_batch=0

            # If this is the first iteration we start by writing the headers
            if header==[]:
                for result in first_query_results:
                    for title in first_query_results[result]:
                        if title not in header:
                            header.append(title)
                for result in current_results:
                    for title in current_results[result][0]:
                        if title not in header:
                            header.append(title)
                writer.writerow(header)

            # Adding results to the main results dictionary:
            for result in current_results:
                query_results_to_write[result]=first_query_results[result]
                for title in current_results[result][0]:
                    try:
                        if isinstance(current_results[result][0][title],list)==True:
                            try:
                                query_results_to_write[result][title]=" ".join(current_results[result][0][title])
                            except:
                                query_results_to_write[result][title]=" ".join(map(str,current_results[result][0][title]))
                        else:
                            query_results_to_write[result][title]=current_results[result][0][title]
                    except:
                        pass

            # At last, writing the results from this batch into the csv:
            for result in query_results_to_write:
                row=[]
                for title in header:
                    try:
                        if isinstance(query_results_to_write[result][title],float)==True:
                            row.append(str(query_results_to_write[result][title]).encode("ascii","ignore"))
                        elif isinstance(query_results_to_write[result][title],list)==True:
                            try:
                                row.append((" ".join((result[title]))).encode("ascii","ignore"))
                            except:
                                row.append((" ".join(map(str,result[title]))).encode("ascii","ignore"))
                        else:        
                            row.append(query_results_to_write[result][title].encode("ascii","ignore"))
                    except:
                        row.append("")
                writer.writerow(row)

            # Cleaning the dictionaries for the next batch
            current_results={}
            query_results_to_write={} 


client.disconnect()


print "\nfinished"
