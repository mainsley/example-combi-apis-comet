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

import importio

output_name = sys.argv[1] if len(sys.argv) > 1 else raw_input("Name of the output CSV file you want to create: ")

# First of all we need to read the config file:
config = ConfigParser.ConfigParser()
config.read("config_options.ini")
username=config.get("combi_apis","username")
password=config.get("combi_apis","password")
extractor_guid_1=config.get("combi_apis","extractor_guid_1")
extractor_guid_2=config.get("combi_apis","extractor_guid_2")
input_second_extractor=config.get("combi_apis","input_second_extractor")
# If the config file has a starting url, the script will use that. If not, it will ask the user for a csv file to load a list of urls.
try:
    starting_url=config.get("combi_apis","starting_url")
except:
    file_with_urls = raw_input("Name of CSV file containing the input urls: ")
    starting_url=[]
    with open(file_with_urls+".csv", "rb") as infile:
        reader=csv.reader(infile)
        for row in reader:
            starting_url.append(row[0])

# We define a latch class as python doesn't have a counting latch built in
class _Latch(object):
  def __init__(self, count=1):
    self.count = count
    self.lock = threading.Condition()

  def countDown(self):
    with self.lock:
      self.count -= 1

      if self.count <= 0:
        self.lock.notifyAll()

  def await(self):
    with self.lock:
      while self.count > 0:
        self.lock.wait()

logging.basicConfig(level=logging.INFO)

current_results={}
def callback(query, message):
  global current_results
  if message["type"] == "MESSAGE": 
    current_results[message["data"]["pageUrl"]]=message["data"]["results"]    
  if query.finished(): latch.countDown()

# Initialise the library
client = importio_client.ImportIO()
client.login(username, password)
client.connect()

# Now we are going to query the first extractor
print "Querying the first extractor:"
# If the input for the first extractor is onyl one:
if isinstance(starting_url,list)==False:
    # Use a latch to stop the program from exiting
    latch = _Latch(1)

    # Querying extractor 1:
    client.query({
      "connectorGuids": [
        extractor_guid_1
      ],
      "input": {
        "webpage/url": starting_url
      }
    }, callback)

    # Wait until queries complete
    latch.await()

    # Here we create a list with all outputs from extractor 1 that we are going to use as inputs in extractor 2.
    inputs_second_extractor=[]
    first_query_results={}
    for result in current_results[starting_url]:
        inputs_second_extractor.append(result[input_second_extractor])
        # We re-organize the first results into a dictionary using second extractor inputs as keys.
        first_query_results[result[input_second_extractor]]=result

# If the input for the first extractor is a list of urls, we iterate the query, making 10 queries at once
else:
    len_last_batch=len(starting_url)%10
   # Use a latch to stop the program from exiting
    latch = _Latch(10)

    # Querying extractor 1:
    num_queries_in_batch=0
    inputs_second_extractor=[]
    first_query_results={}
    for input_ in starting_url:
        print "Query #%s of %s " % (starting_url.index(input_), len(starting_url))
        queries_to_made=len(starting_url)-starting_url.index(input_)
        if queries_to_made==len_last_batch:
            latch = _Latch(len_last_batch)
        client.query({
          "connectorGuids": [
            extractor_guid_1
          ],
          "input": {
            "webpage/url": input_
          }
        }, callback)
        num_queries_in_batch=num_queries_in_batch+1 
        print queries_to_made
        # Wait until queries complete
        if num_queries_in_batch==10 or queries_to_made==1:
            latch.await()
            num_queries_in_batch=0
            # Here we create a list with all outputs from extractor 1 that we are going to use as inputs in extractor 2.
            for url in current_results:
                for result in current_results[url]:
                    inputs_second_extractor.append(result[input_second_extractor])
                    # We re-organize the first results into a dictionay using second extractor inputs as keys.
                    first_query_results[result[input_second_extractor]]=result

print first_query_results
# Defining new Latch to do ten queries at once for the second extractor:
latch = _Latch(10)

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
            latch = _Latch(len_last_batch)
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
            latch.await()
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
                    if isinstance(current_results[result][0][title],list)==True:
                        try:
                            query_results_to_write[result][title]=" ".join(current_results[result][0][title])
                        except:
                            query_results_to_write[result][title]=" ".join(map(str,current_results[result][0][title]))
                    else:
                        query_results_to_write[result][title]=current_results[result][0][title]

            # At last, writing the results from this batch into the csv:
            for result in query_results_to_write:
                row=[]
                for title in header:
                    if isinstance(query_results_to_write[result][title],float)==True:
                        row.append(str(query_results_to_write[result][title]).encode("ascii","ignore"))
                    elif isinstance(query_results_to_write[result][title],list)==True:
                        try:
                            rows[i].append((" ".join((result[title]))).encode("ascii","ignore"))
                        except:
                            rows[i].append((" ".join(map(str,result[title]))).encode("ascii","ignore"))
                    else:        
                        row.append(query_results_to_write[result][title].encode("ascii","ignore"))
                writer.writerow(row)

            # Cleaning the dictionaries for the next batch
            current_results={}
            query_results_to_write={} 


client.disconnect()


print "\nfinished"