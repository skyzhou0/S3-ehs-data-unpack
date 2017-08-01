
from pandasql import *
from pandas import *

#import pandas as pd
pysqldf = lambda q: sqldf(q, globals())

# Define file paths.
allFileNames = '/home/ubuntu/allfiles/allfiles.txt'
inputFilePath = '/home/ubuntu/input-ehs-data/'
ouputFilePath = '/home/ubuntu/output-ehs-data/'

# lines = [line.rstrip('\n') for line in open('/Users/haozhou/Desktop/allS3FileNames.txt')]

# lines = [line.rstrip('\n') for line in open('/Users/haozhou/Desktop/allfiles.txt')]

q = """
	select dt, TriggeredSendExternalKey, CampaignID, count(EmailAddress) volume
	from
	(
	select a.EmailAddress, a.TriggeredSendExternalKey, a.CampaignID,
		 substr(EventDate, 1, instr(EventDate, ' ')) as dt 
	from df a 
	)
	group by
	dt, TriggeredSendExternalKey, CampaignID
	"""

def fileId(i):
	return "dfAggregateBatch3"+str(i)+".csv.gz"

# def fileNameList(listName):

# 	newLinesList = []

# 	for i in xrange(len(lines)):
# 		if '.gz' in lines[i]:
# 			newLinesList.append(lines[i][32:len(lines[i])])

# 	return newLinesList


def main():

	lines = [line.rstrip('\n') for line in open(allFileNames)]

	# linesFileNames = fileNameList(lines)

	N = len(lines)
	print N

	for i in xrange(N):
		fileNamePath = inputFilePath + lines[i]

		# df = pd.read_csv(fileNamePath, compression='gzip', header=0, sep='\t', quotechar='"',  iterator=True, chunksize=50000)


		tp = pd.read_csv(fileNamePath, compression='gzip', header=0, sep='\t', quotechar='"',  iterator=True, chunksize=50000)
		df = concat(tp, ignore_index=True)
		dfAggregate = sqldf(q, locals())

		# write the data object to a file.
		outputFileName = ouputFilePath + fileId(i)

		dfAggregate.to_csv(outputFileName, sep='|', float_format=None, header=True,  index=False, compression='gzip') #encoding='ascii',
		print i


if __name__ == '__main__':
	main()

# the End.



# fileNamePath = inputFilePath + lines[1]
# df = pd.read_csv(fileNamePath, compression='gzip', header=0, sep='\t', quotechar='"')


# df = pd.read_csv('/Users/haozhou/Desktop/Sent_20161102.txt.gz', compression='gzip', header=0, sep='\t', quotechar='"')
# df = df[['EmailAddress', 'EventDate', 'TriggeredSendExternalKey', 'CampaignID']]

# dfAggregate = sqldf(q, locals())

# dfAggregate.to_csv('dfAggregate.csv', sep='|', float_format=None, header=True, encoding='ascii', index=False)
