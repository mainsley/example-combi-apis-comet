example-combi-apis-comet
========================

Combining connectors and extractors to get you the data you need

The Python script combines two APIs. The first can be either an Extractor or a Connector which you use to extract the links (URLs) from the original page of results. Then it passes those URLs through the second API — which must take URLs as its input and therefore be an Extractor, not a Connector — which you build to the page you want the data from. 

To use the script you need Python 2.7 installed in your machine and our Python client library  (https://github.com/import-io/importio-client-libs/blob/master/python/importio).

You'll need to edit the .ini file to enter your own APIs. You can find the GUIDs of your APIs on your "My Data" page. In "input_first_extractor" you need to enter the name you give to the input of that API (in the case of a Connector) or "webpage/url" in the case of an extractor. In "input_second_extractor" you need to give the name of the column in the first API that you want to use as an input for the second API. The last line, "starting_url" is to add the input or url for the first API. In case you have more than one url/query, you can delete that last line of the config file, and when executed, the script will ask you for the csv file containing all the urls/queries you want to query to the first API.

Then you just need to execute the script like so.
The script will ask you to name the output file where it is going to write the results. It will then do all the queries needed in the first API and then in the second API, saving the results in a CSV.  By using asynchronous requests the script will do 10 queries at once, which you can read more about here. 

