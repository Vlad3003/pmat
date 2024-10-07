#!/usr/bin/python3
from sys import stdin, stderr


def greeting(_name: str) -> None:
    if not _name[0].isupper():
        raise ValueError(f"Name '{_name}' needs to start uppercase!")

    for sym in _name:
        if not sym.isalpha():
            raise ValueError(f"Name '{_name}' contains an invalid character '{sym}'!")

    print(f"Nice to see you {_name}!")


if __name__ == "__main__":
    if stdin.isatty():
        work = True

        while work:
            try:
                name = input("Hey, what's your name?\n")
                greeting(name)

            except ValueError as e:
                stderr.write(f"Error: {e}\n")

            except KeyboardInterrupt:
                work = False
                print("\nGoodbye!")

    else:
        for name in stdin:
            try:
                greeting(name.strip())

            except ValueError as e:
                stderr.write(f"Error: {e}\n")
