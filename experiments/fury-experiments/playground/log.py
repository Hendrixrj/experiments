import logging
import sys

class Logger:

    def __init__(self, name, level):

    	self.level = level

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        consolehandler = logging.StreamHandler(sys.stdout)
        consolehandler.setLevel(level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        consolehandler.setFormatter(formatter)

        self.logger.addHandler(consolehandler)

    def stdout(self, message):

        if (self.level == logging.INFO):
            self.logger.info(message)

        if (self.level == logging.DEBUG):
            self.logger.debug(message)

        if (self.level == logging.WARN):
            self.logger.warn(message)

        if (self.level == logging.ERROR):
            self.logger.error(message)