# pmat
___

## Лабораторная работа №1

1. Переходим в lab1.
```shell
cd lab1
```

2. Делаем файлы random_number.py, divide.py, sqrt.py исполняемыми.
```shell
chmod +x random_num.py
chmod +x divide.py
chmod +x sqrt.py
```

3. Запускаем.
```shell
./random_num.py | ./divide.py 2>>errors.txt | ./sqrt.py 2>>errors.txt
```
___

## Лабораторная работа №2

1. Переходим в lab2
```shell
cd lab2
```

2. Делаем файл greeting.py исполняемым.
```shell
chmod +x greeting.py
```

__Пример 1__

```shell
cat names.txt
```
```text
Nick
Anna
Ivan
Logan
maria
Va-nya
```

```shell
./greeting.py < names.txt 2> error.txt
```
```text
Nice to see you Nick!
Nice to see you Anna!
Nice to see you Ivan!
Nice to see you Logan!
```

```shell
cat error.txt
```
```text
Error: Name 'maria' needs to start uppercase!
Error: Name 'Va-nya' contains an invalid character '-'!
```

__Пример 2__

```shell
./greeting.py
```
```text
Hey, what's your name?
Vlad
Nice to see you Vlad!
Hey, what's your name?
Andrey
Nice to see you Andrey!
Hey, what's your name?
Ilya
Nice to see you Ilya!
Hey, what's your name?
^C
Goodbye!
```
