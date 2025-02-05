from Logging.Logger import Logger
import os

HEADER_HALF_LENGTH = 30
logger = Logger("main.py", "API_LYNN_PYTHON")

#Error Constants
FATAL_REQUEST = "FATAL_REQUEST"
REQUEST_NO_RESPONSE = "REQUEST_NO_RESPONSE"
STATUS_CODE_ERROR = "STATUS_CODE_ERROR"

logger.add_standard_error("FATAL_REQUEST", lambda args: "Fatal Error occurred while sending the request")
logger.add_standard_error("REQUEST_NO_RESPONSE", lambda args: "Invalid response provided. Got no response")
logger.add_standard_error("STATUS_CODE_ERROR", lambda args: "Invalid response provided. Got response code " + str(args[0]))
logger.add_logging_template("Title", lambda message, line_num: ("-" * HEADER_HALF_LENGTH) + message + ("-" * HEADER_HALF_LENGTH))