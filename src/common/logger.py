import logging
import sys
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger(__name__)

# Formatters
formater = logging.Formatter(
    fmt="[%(asctime)s.%(msecs)03d] %(levelname)-7s %(module)10s:%(lineno)-3d  - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Handlers
stream_handler = logging.StreamHandler(sys.stdout)

file_handler = logging.FileHandler("logs/root_logger.log", encoding="utf-8", mode="a")

time_rotating_file_handler = TimedRotatingFileHandler(
    "logs/app.log",
    when="midnight",  # ротация каждый день в полночь
    interval=1,
    backupCount=7,  # хранить 7 последних файлов
)
time_rotating_file_handler.suffix = "%Y-%m-%d"  # добавит дату в имя файла


stream_handler.setFormatter(formater)
file_handler.setFormatter(formater)
time_rotating_file_handler.setFormatter(formater)

logger.handlers = [
    stream_handler,
    # file_handler,
    time_rotating_file_handler,
]

logger.setLevel(logging.INFO)
