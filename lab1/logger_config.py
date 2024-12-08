import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="logs.txt",
    filemode="a",
    format="%(message)s (%(filename)s)",
    level=logging.DEBUG,
)
