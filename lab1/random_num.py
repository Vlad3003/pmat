#!/usr/bin/python3
from random import randint

from logger_config import logger


def main():
    a = randint(-10, 10)
    logger.debug(f"A={a}")
    print(a)


if __name__ == "__main__":
    main()
