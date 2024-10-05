#!/usr/bin/python3
import traceback
from math import sqrt

def main():
    try:
        num = int(input())
        res: float = sqrt(num)

        with open("output.txt", "a") as file:
            file.write(f"{res}\n")

    except ValueError:
        with open("errors.txt", "a") as file:
            file.write(traceback.format_exc())


if __name__ == "__main__":
    main()
