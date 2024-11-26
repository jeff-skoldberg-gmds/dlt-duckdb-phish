import logging

def logging_setup():
    logger = logging.getLogger("dlt")
    # Create a timed rotating file handler
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename="pipeline.log", when="midnight", interval=1, backupCount=2
    )
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.info("Logging setup complete")
    return logger

logger = logging_setup()