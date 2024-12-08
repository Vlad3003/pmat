#!/usr/bin/python3
from random import randint
from sys import stderr

from logger_config import logger


def main():
    try:
        a = int(input())
        b = randint(-10, 10)
        logger.debug(f"A={a}, B={b}")

        res = a / b
        print(res)

    except (ZeroDivisionError, ValueError) as exc:
        print(exc, file=stderr)
        exit(1)


if __name__ == "__main__":
    main()
