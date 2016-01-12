# funneler_py
A python script that recursively generates a BigQuery-dialect SQL query to build a funnel against Google Analytics page-hit data

To use:

1. Create an input file called 'input.json' - it should contain one json that looks like this:

{
	"table": "[(dataset id).ga_sessions_]",
	"start": "'2015-11-01'", # or any start date
	"end": "'2015-11-02'", # or any end date
	"regex_list": ["'regex #1'",
		"'regex #2'",
		"'regex #3'",
		"'regex #4'"],
	"segmode": "True", # or False, whatever you like
	"segment": "device.deviceCategory",
	"filtermode": "True", # or False, whatever you like
	"filtercol" : "trafficSource.medium",
	"filterval" : "'organic'"
}

2. run 'python funneler.py input.json'
