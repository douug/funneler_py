"""

Unordered Funneler

Recursive GA funnel query writer for BigQuery with optional segmenting and
filtering

Input json format (no parentheses in actual input):

{
	"table": "[(Dataset ID).ga_sessions_]",
	"start": "'(YYYY-MM-DD)'",
	"end": "'(YYYY-MM-DD)'",
	"regex_list":
		["'(regex #1)'",
		"'(regex #2)'",
		"'(regex #3)'"],
	"segmode": "True" for segmenting, "False" otherwise,
	"segment": "(a column to segment on)",
	"filtermode": "True" for filtering, "False" otherwise,
	"filtercol" : "(a column to filter on)",
	"filterval" : "'(the value to filter on)'"
}

Please note quoted quotes - these are because the Python strings may contain
SQL strings. So if eg filterval is 23, inside quotes aren't required because
23 is of type int in SQL and wouldn't be quoted.

"""

import sys
import time
import json

class Funneler(object):

	# Unpack input json:
	def __init__(self, infile, outfile):
		with open(infile) as inf:
			inp = json.loads(open(infile).read())
			self.table = inp['table']
			self.start_date = inp['start']
			self.end_date = inp['end']
			self.regex_list = inp['regex_list']
			self.segmode = inp['segmode'] == 'True'
			self.seg = inp['segment']
			self.filtermode = inp['filtermode'] == 'True'
			self.filtercol = inp['filtercol']
			self.filterval = inp['filterval']
			print('Filtermode: ' + str(self.filtermode))
			print('Filter Column: ' + str(self.filtercol))
			print('Filter Value: ' + str(self.filterval))
			print('Segmode: ' + str(self.segmode))
			print('Segment: ' + str(self.seg))
			print('Query is in ' + outfile)
		self.outfile_name = outfile

	# Initialise:
	def funnel(self):

		start_cols = ['s0.fullVisitorId','s0.visitId']

		if self.segmode:
			start_cols.append('s0.' + self.seg)

		return self.helper(self.regex_list, '', len(self.regex_list), 0, 's0', start_cols)

	# Helper method:
	def helper(self, regex_list, subquery, orig, counter, name, columns):

		# Outer query which does aggregation:
		if len(regex_list) == 0:

			names_index = list(zip(map(str,range(counter)), self.sanitise(self.regex_list)))
			column_list = list(map((lambda x: 'COUNT(s' + x[0] + '.firstHit) AS ' + x[1] + ', SUM(s' + x[0] + '.exit) AS ' + x[1] + '_exits'), names_index))

			if self.segmode:
				column_list = ['s0.'+self.seg] + column_list

			column_str = ', '.join(column_list)

			if self.segmode:
				groupby = 'GROUP BY ' + 's0.'+self.seg
				orderby = 'ORDER BY ' + names_index[-1][1] + ' DESC'
				new_subquery = ' '.join(['SELECT',column_str,'FROM','('+subquery+')',name, groupby, orderby])
			else:
				new_subquery = ' '.join(['SELECT',column_str,'FROM','('+subquery+')',name])

			with open(self.outfile_name, 'w') as out: out.write(new_subquery)

		# Base case - return stage_view of first regex in regex_list:
		elif len(regex_list) == orig:
			columns.append('s0.firstHit')
			columns.append('s0.exit')
			subquery = self.stage_view(regex_list[0], counter)
			return self.helper(regex_list[1:], subquery, orig, counter+1, name, columns)

		# Inductive case - return stage view of current regex joined to subquery:
		else:
			stage = self.stage_view(regex_list[0], counter)
			columns = self.columns(columns, counter, name)
			new_subquery = self.query(subquery, stage, counter, columns, name)
			return self.helper(regex_list[1:], new_subquery, orig, counter+1, name+str(counter), columns)

	# Returns list of columns to SELECT
	def columns(self, columns, counter, name):
		columns.append('s'+str(counter)+'.firstHit')
		columns.append('s'+str(counter)+'.exit')
		return columns

	# Returns current query
	def query(self, subquery, stage, counter, columns, name):
		cols = ', '.join(columns)
		table = ' '.join(['('+subquery+')',name,'FULL OUTER JOIN EACH','('+stage+')','s'+str(counter),'ON','s0.fullVisitorId','=','s'+str(counter)+'.fullVisitorId','AND','s0.visitId','=','s'+str(counter)+'.visitId'])
		return ' '.join(['SELECT', cols, 'FROM', table])

	# Returns query on table (rather than subquery) - selects rows that match a regex
	def stage_view(self, regex, counter):
		if self.segmode:
			cols = 'fullVisitorId, visitId, ' + self.seg + ', MIN(hits.hitNumber) AS firstHit, MAX(IF(hits.isExit, 1, 0)) as exit'
			groupby = 'GROUP BY fullVisitorId, visitId, ' + self.seg
		else:
			cols = 'fullVisitorId, visitId, MIN(hits.hitNumber) AS firstHit, MAX(IF(hits.isExit, 1, 0)) as exit'
			groupby = 'GROUP BY fullVisitorId, visitId'

		if self.filtermode:
			where = 'WHERE REGEXP_MATCH(hits.page.pagePath, ' + regex + ') AND totals.visits = 1 AND ' + self.filtercol + ' = ' + self.filterval
		else:
			where = 'WHERE REGEXP_MATCH(hits.page.pagePath, ' + regex + ') AND totals.visits = 1'

		table = 'TABLE_DATE_RANGE(' + self.table + ', TIMESTAMP(' + self.start_date + '), TIMESTAMP(' + self.end_date + '))'
		return ' '.join(['SELECT', cols, 'FROM', table, where, groupby])

	# Processes strings so they are suitable to be column names
	def sanitise(self, los):
		return map((lambda x: x.replace('-', '_').replace('/', '_').replace("'", '').replace("(", '').replace(")", '').replace("|", '_')), los)

if __name__ == '__main__':

	funneler = Funneler(sys.argv[1], (time.strftime('%a_%d%m%y', time.localtime()) + '_query.txt'))
	funneler.funnel()
