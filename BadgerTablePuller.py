import datetime
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta

import main
from Logging.ActiveLogger import logger
from main import getStatusUrl, getReportDownloaded

MONTH = 1
DAY = 2


class BadgerTablePuller():
    def __init__(self, startDate, amount_of_time, units=MONTH):
        self.startDate = startDate
        self.amount_of_time = amount_of_time
        self.units_of_time = units
        self.username, self.password = main.getCredentials()
        print(self.username)
        print(self.password)

    def _pull(self, startDate, endDate):
        logger.log("BeaverWorks Report Downloader", "Title")
        statusUrl = getStatusUrl(self.username, self.password, startDate, endDate)
        if self.units_of_time == DAY:
            filename = "reports/rolling-report.csv"
        else:
            filename = "reports/monthly-report-from-{0}.csv".format(startDate)
        getReportDownloaded(statusUrl, self.username, self.password,
                            filename=filename)
        logger.log("Completed")
        print("Pulled {0}".format(filename))
        logger.log("Pulled {0}".format(filename))
        # logger.close()

    def run(self):
        if self.units_of_time == MONTH:
            delta = relativedelta(months=1)
            timeTilEndDate = relativedelta(months=self.amount_of_time)
        elif self.units_of_time == DAY:
            delta = relativedelta(weeks=1)
            timeTilEndDate = relativedelta(days=self.amount_of_time)
        else:
            raise Exception("Invalid unit of time provided {0}".format(self.units_of_time))

        endDate = self.startDate + timeTilEndDate
        currentDate = self.startDate
        while currentDate < endDate:
            print("pull")
            self._pull(currentDate, currentDate + delta)
            currentDate = currentDate + delta

    def run_parallel(self):

        if self.units_of_time != MONTH:
            raise Exception("Parallel mode only supported for months")

        executor = ThreadPoolExecutor(5)
        results = []

        delta = relativedelta(months=1)
        endDate = self.startDate + relativedelta(months=self.amount_of_time)
        currentDate = self.startDate
        while currentDate <= endDate:
            print("pull")
            results.append(executor.submit(self._pull, currentDate, currentDate + delta))
            currentDate = currentDate + delta

        for result in results:
            result.result()

#Get last week's start point
year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day

#last_week_start = datetime.date.today() - relativedelta(weeks=1)
last_week_start = datetime.datetime(2022, 3, 2)
puller = BadgerTablePuller(date(last_week_start.year, last_week_start.month, last_week_start.day), 7, DAY)
puller.run()
# puller.run_parallel()

