import logging
import sys
import json
from functools import partial
import uuid
import os


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        # include extra fields
        if hasattr(record, "layer"):
            log_record["layer"] = record.layer
        if hasattr(record, "job"):
            log_record["job"] = record.job
        if hasattr(record, "run_id"):
            log_record["run_id"] = record.run_id
        if hasattr(record, "dataset"):
            log_record["dataset"] = record.dataset

        return json.dumps(log_record)


def set_up_logger(log_to_file=False, log_file=None):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    if not logger.handlers:
        if log_to_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger


def get_job_logger(logger, layer=None, job=None, dataset=None, run_id=None):

    def log(level, message, **kwargs):
        logger.log(level,
                   message,
                   extra={
                       "layer": layer,
                       "job": job,
                       "run_id": run_id,
                       "dataset": dataset, **kwargs
                   }
                   )

    return log
