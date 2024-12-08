#!/usr/bin/python3
from math import sqrt
from sys import stderr

from logger_config import logger


def main():
    try:
        num = float(input())
        logger.debug(f"A={num}")

        res = sqrt(num)

        with open("output.txt", "a") as file:
            file.write(f"{res}\n")

    except (EOFError, ValueError) as exc:
        print(exc, file=stderr)
        exit(1)


if __name__ == "__main__":
    main()
