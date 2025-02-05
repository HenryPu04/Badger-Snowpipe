import requests
import csv
def fixQuotesInCSV(tmpFilename, filename):
    writer = csv.writer(open(filename, "w"), quoting=csv.QUOTE_NONE, escapechar="\n")
    reader = csv.reader(open(tmpFilename, "r"), skipinitialspace=True)
    writer.writerows(reader)
    # print(len(list(reader)))
    print(reader)

fixQuotesInCSV("reports/rolling-report-tmp.csv", "reports/huh.csv")