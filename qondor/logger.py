import logging
import qondor

def setup_logger(name='qondor', fmt=None):
    if name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.info('Logger %s is already defined', name)
    else:
        if fmt is None:
            fmt = logging.Formatter(
                fmt = (
                    qondor.colored(
                        '[{0}|%(levelname)8s|%(asctime)s|%(module)s]:'.format(name),
                        'yellow'
                        )
                    + ' %(message)s'
                    ),
                datefmt='%Y-%m-%d %H:%M:%S'
                )
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger


def setup_subprocess_logger():
    return setup_logger(
        'subprocess',
        fmt = logging.Formatter(
            fmt = qondor.colored('[%(asctime)s]:', 'red') + ' %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
            )
        )


def set_log_file(
        log_file,
        logger_name='qondor',
        subprocess_logger_name='subprocess'
        ):
    """
    Also outputs all logging to a file, but keeps the output
    to stderr as well.
    """
    log_file = osp.abspath(log_file)

    logger = logging.getLogger(logger_name)
    subprocess_logger = logging.getLogger(subprocess_logger_name)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(LOGGER_FORMATTER)
    logger.addHandler(file_handler)

    # Little bit dangerous; not sure whether logging will open the same file twice
    subprocess_file_handler = logging.FileHandler(log_file)
    subprocess_file_handler.setFormatter(SUBPROCESS_LOGGER_FORMATTER)
    subprocess_logger.addHandler(subprocess_file_handler)

