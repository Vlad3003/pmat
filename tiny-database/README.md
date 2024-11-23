# tiny-sql
Минималистичная СУБД, хранящаяся в CSV.


## Установка poetry
Можно использовать любой другой инструмент для управления зависимостями и виртуальными окружениями.
```aiignore
curl -sSL https://install.python-poetry.org | python3 -
```

## Задание №1
1. В таблицу **EmployeeTable** внести индекс _department_id_.
2. Добавить функцию select для **DepartmentTable** по названию подразделения (возвращать список, т.к. таких может быть несколько)
3. Сделать проверку на то, что в каждой из таблиц присутствуют только уникальные наборы индексов по кортежам.
   То есть в таблице **EmployeeTable** не должно присутствовать ни одной записи с одинаковыми _id_ и _department_id_:
если для любых двух записей в таблице EmployeeTable _id_1_ == _id_2_ и _department_id_1_ == _department_id_2_, тогда это одни и
   те же записи (по сути условие уникального композитного индекса). Для таблицы **DepartmentTable** гарантировать, что нет двух
   записей с одинаковыми id.
4. Реализовать в **Database** классе функцию _join_, которая будет объединять таблицы по полям '_DepartmentTable.id_' и 
'_EmployeeTable.department_id_'.
5. Написать тесты:
   1. На проверку уникальных индексов для пункта 2,
   2. Тест для проверки правильности операции **JOIN** из пункта 3.
   3. Написать тест для _select_ метода таблицы **EmployeeTable**.

## Команды
Запустить тесты c покрытием кода:
```
poetry run pytest --cov
```