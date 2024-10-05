#!/usr/bin/python3
import traceback
from random import randint

def main():
    try:
        a = int(input())
        b = randint(-10, 10)

        res: float = a / b
        print(res)

    except (ZeroDivisionError, ValueError):
        with open("errors.txt", "a") as file:
            file.write(traceback.format_exc())


if __name__ == "__main__":
    main()
