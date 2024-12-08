import csv
import os
from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Callable, Literal, Optional, Union


class SingletonMeta(type):
    """Синглтон метакласс для Database."""

    _instances: dict = {}

    def __call__(cls, *args, **kwargs) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=SingletonMeta):
    """Класс-синглтон базы данных с таблицами, хранящимися в файлах."""

    _aggregate_functions: dict[str, Callable] = {
        "SUM": sum,
        "AVG": lambda x: sum(x) / len(x),
        "MIN": min,
        "MAX": max,
        "COUNT": len,
    }

    _RT = dict[str, str]

    def __init__(self) -> None:
        self.tables: dict[str, "Table"] = {}

    def register_table(self, table_name: str, table: "Table") -> None:
        self.tables[table_name] = table

    def insert(
        self, table_name, data, *, sep: Literal[" ", ","] = " "
    ) -> None:
        table = self.tables.get(table_name)
        if table:
            table.insert(data, sep=sep)
        else:
            raise ValueError(f"Table '{table_name}' does not exist.")

    def select(self, table_name: str, *args, **kwargs) -> Optional[list[_RT]]:
        table = self.tables.get(table_name)
        return table.select(*args, **kwargs) if table else None

    def join(
        self, tables: tuple[str, ...], join_attrs: list[tuple[str, str]]
    ) -> list[_RT]:
        """
        Объединяет несколько таблиц по указанным аттрибутам.

        :param tables: Кортеж имен таблиц.
        :param join_attrs: Список кортежей, где каждый кортеж представляет
        пары 'таблица.атрибут' для соединения.

        :return: Результат объединения таблиц, если удалось объединить
        таблицы по указанным атрибутам, иначе пустой список.
        """
        if len(tables) < 2:
            raise ValueError(
                "At least two tables are required to perform a join."
            )

        tables_objects = [self.tables.get(table_name) for table_name in tables]

        for table_name, table in zip(tables, tables_objects):
            if not table:
                raise ValueError(f"Table '{table_name}' does not exist.")

        if len(join_attrs) != len(tables) - 1:
            raise ValueError(
                "Number of join attributes must be "
                "one less than the number of tables."
            )

        for attrs in join_attrs:
            if len(attrs) != 2:
                raise ValueError(
                    "The join_attrs elements must consist of 2 "
                    "elements with which the tables will be linked"
                )

            for attr in attrs:
                if not attr.count("."):
                    raise ValueError(
                        f"Join attribute '{attr}' must have the "
                        f"following format 'table_name.table_attribute'."
                    )

                _table_name, _attr = attr.split(".")
                _table = self.tables.get(_table_name)

                if not _table:
                    raise ValueError(f"Table '{_table_name}' does not exist.")

                if _attr not in _table.ATTRS:
                    raise ValueError(
                        f"'{_attr}' is not an "
                        f"attribute of table '{_table_name}'."
                    )

        result: list[dict[str, str]] = [
            {f"{tables[0]}.{key}": value for key, value in row.items()}
            for row in tables_objects[0].data
        ]

        for i in range(1, len(tables_objects)):
            table = tables_objects[i]
            join_attr1, join_attr2 = join_attrs[i - 1]
            join_attr2 = join_attr2.split(".")[1]
            new_result = []

            for row1 in result:
                for row2 in table.data:
                    if row1[join_attr1] == row2[join_attr2]:
                        new_row2 = {
                            f"{tables[i]}.{key}": value
                            for key, value in row2.items()
                        }
                        merged_row = {**row1, **new_row2}
                        new_result.append(merged_row)

            result = new_result

        return result

    def aggregate(
        self,
        table_name: str,
        column: str,
        operation: Literal["SUM", "AVG", "COUNT", "MIN", "MAX"],
        *,
        group_by: Optional[str] = None,
    ) -> Union[_RT, list[_RT]]:
        """
        Выполняет агрегацию по указанной таблице и столбцу.

        :param table_name: Имя таблицы
        :param column: Столбец для агрегации
        :param operation: Операция агрегации
        ('SUM', 'AVG', 'COUNT', 'MIN', 'MAX')

        :param group_by: Столбец для группировки
        :return: Результат агрегации
        """
        table = self.tables.get(table_name)

        if not table:
            raise ValueError(f"Table '{table_name}' does not exist.")

        if column not in table.ATTRS:
            raise ValueError(
                f"Column '{column}' does not exist in table '{table_name}'."
            )

        if group_by and group_by not in table.ATTRS:
            raise ValueError(
                f"Column '{group_by}' does not exist in table '{table_name}'."
            )

        aggregate_func = self._aggregate_functions.get(operation)

        if not aggregate_func:
            raise ValueError(f"Operation '{operation}' is not supported.")

        if operation in ("AVG", "SUM") and not all(
            sym.replace(".", "", 1).isdigit()
            for row in table.data
            for sym in row[column]
        ):
            raise TypeError(
                f"The '{operation}' operation cannot be applied to column "
                f"'{column}' as it contains non-numeric data."
            )

        if not group_by:
            values = [
                (
                    float(row[column])
                    if row[column].count(".") == 1
                    else (
                        int(row[column])
                        if all(sym.isdigit() for sym in row[column])
                        else row[column]
                    )
                )
                for row in table.data
            ]

            return {f"{operation}({column})": str(aggregate_func(values))}

        else:
            grouping: dict[str, list[Union[int, float, str]]] = {}

            for row in table.data:
                key = row[group_by]
                if key not in grouping:
                    grouping[key] = []

                value = [
                    (
                        float(row[column])
                        if row[column].count(".") == 1
                        else (
                            int(row[column])
                            if all(sym.isdigit() for sym in row[column])
                            else row[column]
                        )
                    )
                ][0]

                grouping[key].append(value)

            return [
                {
                    f"{group_by}": key,
                    f"{operation}({column})": str(aggregate_func(value)),
                }
                for key, value in grouping.items()
            ]


class Table(ABC):
    """Абстрактный базовый класс для таблиц с вводом/выводом файлов CSV."""

    ATTRS: tuple[str, ...] = ()
    UNIQUE_ATTRS: tuple[Union[str, tuple[str, ...]], ...] = ()
    FILE_PATH: str = ""

    def __init__(self, load_data=True) -> None:
        self.data: list[dict[str, str]] = []

        if load_data:
            self.load()  # Подгружаем из CSV-файла сразу при инициализации

    def save(self) -> None:
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self) -> None:
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]

    @abstractmethod
    def select(self, *args, **kwargs) -> list[dict[str, str]]:
        pass  # pragma: no cover

    def _check_unique(self, new_row: dict[str, str]) -> None:
        """
        Проверка уникальности значений для атрибутов, указанных в UNIQUE_ATTRS.
        Если значение уже существует, выбрасывается исключение.
        """
        for unique_attr in self.UNIQUE_ATTRS:
            if isinstance(unique_attr, str):
                if any(
                    row[unique_attr] == new_row[unique_attr]
                    for row in self.data
                ):
                    raise ValueError(
                        f"Value '{new_row[unique_attr]}' already exists in "
                        f"column '{unique_attr}' of the "
                        f"'{self.__class__.__name__}', which must be unique."
                    )
            else:
                if any(
                    all(row[attr] == new_row[attr] for attr in unique_attr)
                    for row in self.data
                ):
                    values = ", ".join(
                        f"{attr}={new_row[attr]}" for attr in unique_attr
                    )
                    raise ValueError(
                        f"Values '{values}' already exists in "
                        f"columns {unique_attr} of the "
                        f"'{self.__class__.__name__}', which must be unique."
                    )

    def _validate_data(self, new_row: dict[str, str]) -> None:
        """
        Проверяет целостность и корректность данных
        новой строки перед добавлением в таблицу.

        Метод выполняет базовую проверку новой строки `new_row`,
        чтобы убедиться, что количество переданных данных совпадает с
        количеством атрибутов таблицы. Если количество значений не совпадает,
        метод выбрасывает исключение `ValueError`.
        """
        if len(new_row) != len(self.ATTRS):
            raise ValueError(
                f"The number of values in the new row '{len(new_row)}' "
                f"does not match the number of '{self.__class__.__name__}' "
                f"attributes '{len(self.ATTRS)}'."
            )

    def insert(self, data: str, *, sep: Literal[" ", ","] = " ") -> None:
        entry = dict(zip(self.ATTRS, data.split(sep)))
        self._validate_data(entry)
        self._check_unique(entry)
        self.data.append(entry)
        self.save()


class EmployeeTable(Table):
    """Таблица сотрудников с методами ввода-вывода из файла CSV."""

    ATTRS = ("id", "name", "age", "salary", "department_id")
    UNIQUE_ATTRS = ("id", "department_id")
    FILE_PATH = "employee_table.csv"

    def __init__(self, load_data=True) -> None:
        super().__init__(load_data)

    def select(self, start_id: int, end_id: int) -> list[dict[str, str]]:
        return [
            row for row in self.data if start_id <= int(row["id"]) <= end_id
        ]


class DepartmentTable(Table):
    """Таблица подразделений с вводом-выводом в/из CSV файла."""

    ATTRS = ("id", "department_name")
    UNIQUE_ATTRS = ("id",)
    FILE_PATH = "department_table.csv"

    def __init__(self, load_data=True) -> None:
        super().__init__(load_data)

    def select(self, department_name) -> list[dict[str, str]]:
        return [
            row
            for row in self.data
            if row["department_name"] == department_name
        ]


class ProjectTable(Table):
    """Таблица проектов с вводом-выводом в/из CSV файла."""

    ATTRS = ("id", "name", "start_date", "end_date")
    UNIQUE_ATTRS = ("id",)
    FILE_PATH = "project_table.csv"

    def __init__(self, load_data=True) -> None:
        super().__init__(load_data)

    def select(self, start_id: int, end_id: int) -> list[dict[str, str]]:
        return [
            row for row in self.data if start_id <= int(row["id"]) <= end_id
        ]

    def insert(self, data: str, *, sep: Literal[" ", ","] = " ") -> None:
        super().insert(data, sep=",")

    def _validate_data(self, new_row: dict[str, str]) -> None:
        """
        Проверяет корректность данных новой строки перед добавлением
        в таблицу, включая значения и диапазон дат.

        Если значение 'start_date' позже 'end_date', а также,
        если одна из этих дат имеет некорректное значение,
        выбрасывается исключение ValueError
        """
        super()._validate_data(new_row)

        start_date = date.fromisoformat(new_row["start_date"])
        end_date = date.fromisoformat(new_row["end_date"])

        if start_date > end_date:
            raise ValueError(
                f"Invalid date range: 'start_date' ({start_date}) "
                f"cannot be later than 'end_date' ({end_date})."
            )


class EmployeeProjectTable(Table):
    """Таблица проектов сотрудников с вводом-выводом в/из CSV файла."""

    ATTRS = ("employee_id", "project_id", "role")
    UNIQUE_ATTRS = (("employee_id", "project_id"),)
    FILE_PATH = "employee_project_table.csv"

    def __init__(self, load_data=True) -> None:
        super().__init__(load_data)

    def select(
        self,
        employee_id: Optional[int] = None,
        project_id: Optional[int] = None,
    ) -> list[dict[str, str]]:
        if employee_id and project_id:
            result = [
                row
                for row in self.data
                if int(row["employee_id"]) == employee_id
                and int(row["project_id"]) == project_id
            ]
        elif employee_id:
            result = [
                row
                for row in self.data
                if int(row["employee_id"]) == employee_id
            ]
        elif project_id:
            result = [
                row
                for row in self.data
                if int(row["project_id"]) == project_id
            ]
        else:
            result = []

        return result

    def insert(self, data: str, *, sep: Literal[" ", ","] = " ") -> None:
        super().insert(data, sep=",")
