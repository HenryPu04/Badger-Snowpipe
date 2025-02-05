import os
from time import sleep
import csv
import requests
import snowflake.connector
from Logging.ActiveLogger import logger
from datetime import datetime, timedelta

PATH = os.path.dirname(os.path.abspath(__file__))

def getStatusUrl(username, password, startDate = None, endDate = None):
    logger.log("Beginning Status URL Initializations")
    url = "https://api.beaconama.net/v2/eds/range"
    name = username
    pwd = password
    authString = name + ":" + pwd


# // "Account_Full_Name,Account_ID,Backflow (gal/30 days),Battery Level,Billing_Address_Line1," +
# // "Billing_Address_Line2,Billing_Address_Line3,Billing_City,Billing_State,Billing_Zip," +
# // "Connector Type,Current Leak Rate (gal/hr),Current Leak Start Date,Dials," +
# // "Encoder_Read,Endpoint Status,Endpoint_SN,Endpoint_Type,Estimated Flag,Flow,Flow_Time," +
# // "Flow_Unit,Last Comm. Time,Last Gateway Id,Location_Address_Line1,Location_Area," +
# // "Location_Bathrooms,Location_Building_Number,Location_Building_Type,Location_City," +
# // "Location_DHS_Code,Location_District,Location_Funding,Location_ID,Location_Irrigated_Area," +
# // "Location_Irrigation,Location_Main_Use,Location_Name,Location_Pool,Location_Population," +
# // "Location_Site,Location_State,Location_Water_Type,Location_Year_Built,Location_Zip," +
# // "Meter_ID,Meter_Manufacturer,Meter_Model,Meter_SN,Meter_Size,Meter_Size_Desc,Portal_Email," +
# // "Portal_ID,Read,Read_Method,Read_Time,Read_Unit,Register_Number,Register_Resolution," +
# // "Register_Unit_Of_Measure,Service_Point_Class_Code,Service_Point_Cycle,Service_Point_ID," +
# // "Service_Point_Latitude,Service_Point_Longitude,Service_Point_Route,Service_Point_Timezone," +
# // "Signal Strength&" + // these
# are
# all
# the
# columns
# of
# the
# raw
# data
# from API
#     parameters = "startDate=2020-02-12&" + "endDate=2020-02-19&" + "headerColumns=" + \
#     "Account_ID,Register_Number,Meter_ID,Location_Name,Endpoint_SN,Flow," + \
#     "Flow_Unit,Flow_Time,Encoder_Read,Meter_SN&" + \
#     "resolution=quarter_hourly&" + \
#     "unit=Gallons"
    #Today unless otherwise specified
    if not endDate:
        endDate = (datetime.today().strftime('%Y-%m-%d'))

    #One Week ago unless otherwise specified
    if not startDate:
        startDate = (datetime.today() - timedelta(weeks=7)).strftime('%Y-%m-%d')

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    params = {"Start_Date" : str(startDate),
              "End_Date" : str(endDate),
              "Resolution": "quarter_hourly",
              "Unit": "Gallons",
              "Header_Columns":
                  "Account_ID,Register_Number,Meter_ID,Location_Name,Endpoint_SN,Flow," +
                    "Flow_Unit,Flow_Time,Encoder_Read,Meter_SN,Register_Resolution,Endpoint_Status,Battery_Level,"
                    "Signal_Strength, Current_Leak_Start_Date, Current_Leak_Rate"
                    }

    logger.log("Beginning Login attempt")

    try:
        resp = requests.post(url, auth=(name, pwd), data=params, headers=headers)
        if resp.status_code != 200 and resp.status_code != 202:
            logger.error(resp.json())
            raise Exception("Login Error " + str(resp.status_code))
    except Exception as e:
        logger.error("Error made during login attempt")
        logger.error(e)
        logger.close()
        exit()

    resp_json = resp.json()
    logger.log("Status URL: " + str(resp_json["statusUrl"]))

    logger.log("Login Successful")
    return resp_json["statusUrl"]

# Snowflake connection details
SNOWFLAKE_ACCOUNT = 'zp03831.us-east-2.aws'
SNOWFLAKE_USER = 'HYP2'
SNOWFLAKE_PASSWORD = 'Relove1234!'
SNOWFLAKE_DATABASE = 'SANDBOX'
SNOWFLAKE_SCHEMA = 'BADGER'
SNOWFLAKE_STAGE = 'badger_stage'  # Internal or external stage name



# Upload the file to Snowflake stage
def upload_to_snowflake_stage(file_path):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()
    try:
        # Enclose the file path in single quotes and escape special characters
        escaped_file_path = file_path.replace("'", "\\'")
        put_command = f"PUT 'file://{escaped_file_path}' @{SNOWFLAKE_STAGE} OVERWRITE = TRUE;"
        cursor.execute(put_command)
        print(f"Uploaded {file_path} to Snowflake stage {SNOWFLAKE_STAGE}")
    finally:
        cursor.close()
        conn.close()

def load_data_into_table(file_name):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()
    try:
        sanitized_file_name = file_name.replace(" ", "_")  # Optional: sanitize spaces
        copy_command = f"""
        COPY INTO BADGER.report_data
        FROM '@{SNOWFLAKE_STAGE}/{sanitized_file_name}'
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
        """
        cursor.execute(copy_command)
        print(f"Data loaded successfully into BADGER.report_data from {sanitized_file_name}")
    finally:
        cursor.close()
        conn.close()

def trigger_snowpipe(pipe_name):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()
    try:
        cursor.execute(f"ALTER PIPE {pipe_name} REFRESH;")
        print(f"Snowpipe {pipe_name} triggered successfully.")
    finally:
        cursor.close()
        conn.close()

def getReportDownloaded(statusUrl, username, password, filename="report.csv"):
    url2 = "https://api.beaconama.net" + statusUrl
    name = username
    pwd = password

    logger.log("Beginning Report download")
    try:
        logger.log("Retrieving content from the status URL")
        try:
            resp2 = requests.get(url2, auth=(name, pwd))
            if resp2.status_code != 200:
                raise Exception("Error in retrieving content from status URL")
        except Exception as e:
            logger.error("Error made during attempt to retrieve content")
            logger.error(e)
            logger.close()
            exit()

        json_response = resp2.json()
        state = json_response["state"]

        logger.log("Loading file data")
        try:
            while state != "done":
                print(f"Report is not ready yet ({filename})")
                logger.log(f"Report is not ready yet ({filename})")

                sleep(15)  # Wait for 15 seconds before retrying
                resp2 = requests.get(url2, auth=(name, pwd))
                if resp2.status_code != 200:
                    raise Exception("Error Loading the Report Data")
                state = resp2.json()["state"]

        except Exception as e:
            logger.error("Error made during attempt to load file data")
            logger.error(e)
            logger.close()
            exit()

        reportUrl = resp2.json()["reportUrl"]
        fullReportUrl = "https://api.beaconama.net" + reportUrl
        try:
            resp3 = requests.get(fullReportUrl, auth=(name, pwd))
            if resp3.status_code != 200:
                raise Exception("Error retrieving fully loaded report")
        except Exception as e:
            logger.error("Error retrieving fully loaded report")
            logger.error(e)
            logger.close()
            exit()

        # Parse the CSV data and filter out rows with empty data points
        csv_data = resp3.text.splitlines()
        reader = csv.DictReader(csv_data)
        filtered_data = []

        for row in reader:
            # Check for empty data in all columns except for the specified ones
            if all(row[col].strip() for col in row if col not in ["Register_Number", "Current_Leak_Start_Date", "Current_Leak_Rate"]):
                # Set default value for "Current_Leak_Rate" if empty
                if not row["Current_Leak_Rate"].strip():
                    row["Current_Leak_Rate"] = "0.0"
                filtered_data.append(row)

        # Write the filtered data to the output file
        with open(PATH + "/" + filename, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(filtered_data)

        # Upload the final CSV to Snowflake stage
        upload_to_snowflake_stage(PATH + "/" + filename)

        # Load data into the Snowflake table
        load_data_into_table(filename)

        # Trigger Snowpipe if needed
        trigger_snowpipe('my_pipe')

        # Log response headers
        print("Response Headers:", resp3.headers)
        location = resp3.headers.get("Location")
        if location:
            print("Location:", location)
            logger.log(f"Location header found: {location}")
        else:
            print("Location header not found in the response.")
            logger.log("Location header not found in the response.")

    except Exception as e:
        print("Exception occurred:")
        print(e)
        logger.error("Exception occurred during report download")
        logger.error(e)

def getCredentials():
    """
    Reads a credentials file with the following format:
    Filename: credentials.txt
    On Line 1: username
    On Line 2: password
    No other lines should be in the file. Will create file if the file does not exist but you must fill out the information
    :return: username, password
    """
    try:
        print(PATH + "/credentials.txt")
        with open(PATH + "/credentials.txt", "r") as f:
            credentials = [x.strip("\n") for x in f.readlines()]
            print(credentials)
            return credentials
    except FileNotFoundError:
        with open(PATH + "/credentials.txt", "w") as f:
            f.write("<Replace me with username>\n")
            f.write("<Replace me with password>\n")
            raise Exception("ERROR: Credentials.txt was not found so a new one was created. Please fill this file out with a valid"
                  "username and password.")





def main():
    username, password = getCredentials()
    logger.log("BeaverWorks Report Downloader", "Title")
    statusUrl = getStatusUrl(username, password)
    getReportDownloaded(statusUrl, username, password)
    logger.log("Completed")
    logger.close()
main()
