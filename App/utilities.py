#start of utilities.py
import logging
import logging.handlers

def setup_logger(log_file='application.log', level=logging.INFO):
    """Function to setup a shared logger for the entire application"""
    
    # Create a logger
    logger = logging.getLogger('shared_logger')
    logger.setLevel(level)

    # Check if logger already has handlers to avoid duplicate logs
    if not logger.handlers:
        # Create file handler
        file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when='midnight', interval=1)
        file_handler.suffix = "%Y%m%d"
        file_handler.setLevel(level)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# Initialize the shared logger
logger = setup_logger()

#end of utilities.py