import datetime
import inspect
import os
from logging import Logger

"""
Logger class to allow for quick logging to a specified file. 
Logging includes the name of the file, 
"""
class Logger:
    """
    Note that the Logger upon initialization will already begin logging.
    """
    def __init__(self, filename, project_name):
        print("Starting Logger...")
        dir_path = os.path.dirname(os.path.realpath(__file__))

        self.template = str(datetime.datetime.now()) + " "
        self._filename = dir_path + "/" + filename + ".txt"
        self.start_message = "LOGGING INFORMATION FOR THE PROJECT: " + project_name
        self.error_message = "ERROR: "
        self._custom_logging = {"default" : (lambda x,y : "[" + str(y) + "]" + " " + str(x))}
        self.standard_errors = {"default" : (lambda msg, args: msg)}
        try:
            self._log_fp = open(self._filename, "w")
            self._log_fp.write(self.start_message + "\n\n")
        except:
            raise Exception("Logger Error: Logging File failed to be created")



    def log(self, message, logging_type = "default"):
        #Gets the line number of the log
        line_number = inspect.currentframe().f_back.f_lineno

        #Obtain the template to log
        to_be_written = self.get_custom_logging(logging_type)(message, line_number)

        #Write the log to file
        self._log_fp.write(to_be_written)
        self._log_fp.write("\n")

    def generateMessage(self, message, line_number):
        return "[" + str(line_number) + "]" + " " + self.template + message

    def close(self):
        return
        self._log_fp.close()

    def add_logging_template(self, name, new_template):
        self._custom_logging[name] = new_template

    def get_custom_logging(self, logging_type):
        return self._custom_logging[logging_type]

    def error(self, msg):
        self.log(self.error_message + str(msg))

    def add_standard_error(self, name, error):
        self.standard_errors[name] = error

    def standard_error(self, standardErrorName = "default", args =[]):
        self.log(self.error_message + self.standard_errors[standardErrorName](args))



