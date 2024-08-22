class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = 'mdm.entity.dataload'
        self.logger = log4j.LogManager.getLogger(root_class)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warn(self, message):
        self.logger.warn(message)

    def debug(self, message):
        self.logger.debug(message)

    def trace(self, message):
        self.logger.trace(message)