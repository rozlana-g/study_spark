import logging

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

pyspark_log = logging.getLogger('py4j')
pyspark_log.setLevel(logging.ERROR)