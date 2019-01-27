import logging


def config_logger(log):
    log.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    # I0329 09:49:22.792974 2870011 InstanceLog.cpp:348]
    formatter = logging.Formatter(
        "%(levelname)s %(asctime)s.%(msecs)d %(thread)d %(filename)s:%(lineno)d]"
        " %(funcName)s: %(message)s",
        datefmt="%m%dT%H:%M:%S")
    ch.setFormatter(formatter)
    # add the handlers to the log
    log.addHandler(ch)

log = logging.getLogger("default")
config_logger(log)
