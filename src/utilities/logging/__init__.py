import logging
import logging.handlers
import os

logs_dir = os.path.dirname(os.path.realpath(__file__))


def create_logs_dir(logs_dir=logs_dir):
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)


def logging_setup(logs_dir=logs_dir):
    create_logs_dir(logs_dir)
    logger = logging.getLogger("dlt")
    # Retrieve the existing formatter from the dlt logger
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
    else:
        # Fallback formatter if no handlers are present
        existing_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    # Create a timed rotating file handler
    log_file_path = os.path.join(logs_dir, "pipeline.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file_path, when="midnight", interval=1, backupCount=2
    )
    file_handler.setFormatter(existing_formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.info("Logging setup complete")
    return logger


logger = logging_setup(logs_dir=logs_dir)
