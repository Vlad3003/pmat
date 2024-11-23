import pytest
import os
import tempfile
from database.database import (
    Database, EmployeeTable, DepartmentTable,
    ProjectTable, EmployeeProjectTable
)


@pytest.fixture
def temp_employee_file():
    """ Создаем временный файл для таблицы рабочих """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

@pytest.fixture
def temp_department_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

@pytest.fixture
def temp_projects_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

@pytest.fixture
def temp_employees_projects_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

#Пример, как используются фикстуры
@pytest.fixture
def database(temp_employee_file,
             temp_department_file,
             temp_projects_file,
             temp_employees_projects_file):
    """ Данная фикстура задает БД и определяет таблицы. """
    db = Database()

    # Используем временные файлы для тестирования файлового ввода-вывода в EmployeeTable и DepartmentTable
    employee_table = EmployeeTable(load_data=False)
    employee_table.FILE_PATH = temp_employee_file
    department_table = DepartmentTable(load_data=False)
    department_table.FILE_PATH = temp_department_file
    project_table = ProjectTable(load_data=False)
    project_table.FILE_PATH = temp_projects_file
    employee_projects_table = EmployeeProjectTable(load_data=False)
    employee_projects_table.FILE_PATH = temp_employees_projects_file

    db.register_table("employees", employee_table)
    db.register_table("departments", department_table)
    db.register_table("projects", project_table)
    db.register_table("employees_projects", employee_projects_table)

    return db

def test_insert_employee(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")

    # Проверяем вставку, подгружая с CSV
    data = database.select("employees", 1, 2)

    assert len(data) == 2
    assert data[0] == {
        'id': '1', 'name': 'John', 'age': '28',
        'salary': '50000', 'department_id': '1'
    }
    assert data[1] == {
        'id': '2', 'name': 'Jane', 'age': '34',
        'salary': '60000', 'department_id': '2'
    }

def test_unique_attrs_of_the_employee_table(database):
    database.insert("employees", "1 John 28 50000 1")

    with pytest.raises(ValueError):
        database.insert("employees", "1 Jane 34 60000 2")

    with pytest.raises(ValueError):
        database.insert("employees", "2 Jane 34 60000 1")

def test_select_method_of_the_employee_table(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")
    database.insert("employees", "3 Alice 29 45000 3")
    database.insert("employees", "4 Bob 40 70000 4")
    database.insert("employees", "5 Charlie 25 40000 5")
    database.insert("employees", "6 David 30 55000 6")
    database.insert("employees", "7 Emily 38 65000 7")
    database.insert("employees", "8 Frank 50 80000 8")
    database.insert("employees", "9 Grace 33 47000 9")
    database.insert("employees", "10 Helen 29 53000 10")
    database.insert("employees", "11 Ivan 26 48000 11")
    database.insert("employees", "12 Jack 45 72000 12")
    database.insert("employees", "13 Kathy 32 55000 13")
    database.insert("employees", "14 Leo 28 49000 14")
    database.insert("employees", "15 Mona 31 60000 15")
    database.insert("employees", "16 Nathan 37 67000 16")
    database.insert("employees", "17 Olivia 29 52000 17")
    database.insert("employees", "18 Paul 34 59000 18")
    database.insert("employees", "19 Quincy 41 75000 19")
    database.insert("employees", "20 Rachel 33 54000 20")
    database.insert("employees", "21 Sam 27 46000 21")
    database.insert("employees", "22 Tina 39 71000 22")
    database.insert("employees", "23 Ursula 36 63000 23")
    database.insert("employees", "24 Victor 42 77000 24")
    database.insert("employees", "25 Wendy 32 68000 25")

    data = [
        {'id': '1', 'name': 'John', 'age': '28', 'salary': '50000', 'department_id': '1'},
        {'id': '2', 'name': 'Jane', 'age': '34', 'salary': '60000', 'department_id': '2'},
        {'id': '3', 'name': 'Alice', 'age': '29', 'salary': '45000', 'department_id': '3'},
        {'id': '4', 'name': 'Bob', 'age': '40', 'salary': '70000', 'department_id': '4'},
        {'id': '5', 'name': 'Charlie', 'age': '25', 'salary': '40000', 'department_id': '5'},
        {'id': '6', 'name': 'David', 'age': '30', 'salary': '55000', 'department_id': '6'},
        {'id': '7', 'name': 'Emily', 'age': '38', 'salary': '65000', 'department_id': '7'},
        {'id': '8', 'name': 'Frank', 'age': '50', 'salary': '80000', 'department_id': '8'},
        {'id': '9', 'name': 'Grace', 'age': '33', 'salary': '47000', 'department_id': '9'},
        {'id': '10', 'name': 'Helen', 'age': '29', 'salary': '53000', 'department_id': '10'},
        {'id': '11', 'name': 'Ivan', 'age': '26', 'salary': '48000', 'department_id': '11'},
        {'id': '12', 'name': 'Jack', 'age': '45', 'salary': '72000', 'department_id': '12'},
        {'id': '13', 'name': 'Kathy', 'age': '32', 'salary': '55000', 'department_id': '13'},
        {'id': '14', 'name': 'Leo', 'age': '28', 'salary': '49000', 'department_id': '14'},
        {'id': '15', 'name': 'Mona', 'age': '31', 'salary': '60000', 'department_id': '15'},
        {'id': '16', 'name': 'Nathan', 'age': '37', 'salary': '67000', 'department_id': '16'},
        {'id': '17', 'name': 'Olivia', 'age': '29', 'salary': '52000', 'department_id': '17'},
        {'id': '18', 'name': 'Paul', 'age': '34', 'salary': '59000', 'department_id': '18'},
        {'id': '19', 'name': 'Quincy', 'age': '41', 'salary': '75000', 'department_id': '19'},
        {'id': '20', 'name': 'Rachel', 'age': '33', 'salary': '54000', 'department_id': '20'},
        {'id': '21', 'name': 'Sam', 'age': '27', 'salary': '46000', 'department_id': '21'},
        {'id': '22', 'name': 'Tina', 'age': '39', 'salary': '71000', 'department_id': '22'},
        {'id': '23', 'name': 'Ursula', 'age': '36', 'salary': '63000', 'department_id': '23'},
        {'id': '24', 'name': 'Victor', 'age': '42', 'salary': '77000', 'department_id': '24'},
        {'id': '25', 'name': 'Wendy', 'age': '32', 'salary': '68000', 'department_id': '25'}
    ]

    res = database.select("employees", 1, 25)
    assert len(res) == 25
    assert res == data

    res = database.select("employees", 7, 12)
    assert len(res) == 6
    assert res == data[6:12]

    res = database.select("employees", 15, 15)
    assert len(res) == 1
    assert res[0] == data[14]

    res = database.select("employees", 22, 27)
    assert len(res) == 4
    assert res == data[21:]

    res = database.select("employees", 26, 30)
    assert len(res) == 0

def test_insert_department(database):
    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")

    # Проверяем вставку, подгружая с CSV
    data = database.select("departments", "HR")

    assert len(data) == 1
    assert data[0] == {'id': '1', 'department_name': 'HR'}

    data = database.select("departments", "Finance")

    assert len(data) == 1
    assert data[0] == {'id': '2', 'department_name': 'Finance'}

def test_unique_attrs_of_the_department_table(database):
    database.insert("departments", "1 HR")

    with pytest.raises(ValueError):
        database.insert("departments", "1 Finance")

def test_select_method_of_the_department_table(database):
    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("departments", "3 Engineering")
    database.insert("departments", "4 Engineering")
    database.insert("departments", "5 Marketing")
    database.insert("departments", "6 Sales")
    database.insert("departments", "7 Sales")
    database.insert("departments", "8 Sales")

    data = [
        {'id': '1', 'department_name': 'HR'},
        {'id': '2', 'department_name': 'Finance'},
        {'id': '3', 'department_name': 'Engineering'},
        {'id': '4', 'department_name': 'Engineering'},
        {'id': '5', 'department_name': 'Marketing'},
        {'id': '6', 'department_name': 'Sales'},
        {'id': '7', 'department_name': 'Sales'},
        {'id': '8', 'department_name': 'Sales'}
    ]

    res = database.select("departments", "HR")
    assert len(res) == 1
    assert res[0] == data[0]

    res = database.select("departments", "Engineering")
    assert len(res) == 2
    assert res == data[2:4]

    res = database.select("departments", "Sales")
    assert len(res) == 3
    assert res == data[-3:]

    res = database.select("departments", "IT Support")
    assert len(res) == 0

def test_insert_project(database):
    database.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")
    database.insert("projects", "2,CRM Development,2024-02-01,2024-08-01")

    # Проверяем вставку, подгружая с CSV
    data = database.select("projects", 1, 2)

    assert len(data) == 2
    assert data[0] == {
        'id': '1', 'name': 'Website Redesign',
        'start_date': '2024-01-15', 'end_date': '2024-03-15'
    }
    assert data[1] == {
        'id': '2', 'name': 'CRM Development',
        'start_date': '2024-02-01', 'end_date': '2024-08-01'
    }

def test_unique_attrs_of_the_project_table(database):
    database.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")

    with pytest.raises(ValueError):
        database.insert("projects", "1,CRM Development,2024-02-01,2024-08-01")

def test_valid_date_values_of_the_project_table(database):
    with pytest.raises(ValueError):
        database.insert("projects", "1,Website Redesign,2024-03-15,2024-01-15")

    with pytest.raises(ValueError):
        database.insert("projects", "1,Website Redesign,2023-02-29,2024-03-15")

    with pytest.raises(ValueError):
        database.insert("projects", "1,Website Redesign,2024-02-29,2024-13-15")

    with pytest.raises(ValueError):
        database.insert("projects", "1,Website Redesign,2024-02-29,2024-03-32")

def test_select_method_of_the_project_table(database):
    database.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")
    database.insert("projects", "2,CRM Development,2024-02-01,2024-08-01")
    database.insert("projects", "3,HR Automation,2024-01-20,2024-04-20")
    database.insert("projects", "4,Marketing Campaign,2024-03-10,2024-06-10")
    database.insert("projects", "5,Financial Report Tool,2024-02-15,2024-05-15")
    database.insert("projects", "6,Mobile App Development,2024-04-01,2024-09-01")
    database.insert("projects", "7,Data Migration,2024-05-01,2024-07-31")
    database.insert("projects", "8,Internal Wiki,2024-02-05,2024-04-05")
    database.insert("projects", "9,Cloud Infrastructure Setup,2024-06-01,2024-12-01")
    database.insert("projects", "10,Customer Feedback Analysis,2024-07-01,2024-09-01")

    data = [
        {'id': '1', 'name': 'Website Redesign', 'start_date': '2024-01-15', 'end_date': '2024-03-15'},
        {'id': '2', 'name': 'CRM Development', 'start_date': '2024-02-01', 'end_date': '2024-08-01'},
        {'id': '3', 'name': 'HR Automation', 'start_date': '2024-01-20', 'end_date': '2024-04-20'},
        {'id': '4', 'name': 'Marketing Campaign', 'start_date': '2024-03-10', 'end_date': '2024-06-10'},
        {'id': '5', 'name': 'Financial Report Tool', 'start_date': '2024-02-15', 'end_date': '2024-05-15'},
        {'id': '6', 'name': 'Mobile App Development', 'start_date': '2024-04-01', 'end_date': '2024-09-01'},
        {'id': '7', 'name': 'Data Migration', 'start_date': '2024-05-01', 'end_date': '2024-07-31'},
        {'id': '8', 'name': 'Internal Wiki', 'start_date': '2024-02-05', 'end_date': '2024-04-05'},
        {'id': '9', 'name': 'Cloud Infrastructure Setup', 'start_date': '2024-06-01', 'end_date': '2024-12-01'},
        {'id': '10', 'name': 'Customer Feedback Analysis', 'start_date': '2024-07-01', 'end_date': '2024-09-01'}
    ]

    res = database.select("projects", 1, 10)
    assert len(res) == 10
    assert res == data

    res = database.select("projects", 4, 6)
    assert len(res) == 3
    assert res == data[3:6]

    res = database.select("projects", 7, 7)
    assert len(res) == 1
    assert res[0] == data[6]

    res = database.select("projects", 9, 12)
    assert len(res) == 2
    assert res == data[8:]

    res = database.select("projects", 11, 17)
    assert len(res) == 0

def test_insert_employee_project(database):
    database.insert("employees_projects", "1,1,Developer")
    database.insert("employees_projects", "2,1,Project Manager")

    # Проверяем вставку, подгружая с CSV
    data = database.select("employees_projects", project_id=1)

    assert len(data) == 2
    assert data[0] == {
        'employee_id': '1', 'project_id': '1', 'role': 'Developer'
    }
    assert data[1] == {
        'employee_id': '2', 'project_id': '1', 'role': 'Project Manager'
    }

def test_unique_attrs_of_the_employee_project_table(database):
    database.insert("employees_projects", "1,1,Developer")

    with pytest.raises(ValueError):
        database.insert("employees_projects", "1,1,Project Manager")

def test_select_method_of_the_employee_project_table(database):
    database.insert("employees_projects", "1,1,Developer")
    database.insert("employees_projects", "2,1,Project Manager")
    database.insert("employees_projects", "10,1,Consultant")
    database.insert("employees_projects", "20,1,Customer Success Manager")
    database.insert("employees_projects", "1,2,Developer")
    database.insert("employees_projects", "3,2,Tester")
    database.insert("employees_projects", "4,2,Team Lead")
    database.insert("employees_projects", "11,2,UI/UX Designer")
    database.insert("employees_projects", "21,2,Developer")

    data = [
        {'employee_id': '1', 'project_id': '1', 'role': 'Developer'},
        {'employee_id': '2', 'project_id': '1', 'role': 'Project Manager'},
        {'employee_id': '10', 'project_id': '1', 'role': 'Consultant'},
        {'employee_id': '20', 'project_id': '1', 'role': 'Customer Success Manager'},
        {'employee_id': '1', 'project_id': '2', 'role': 'Developer'},
        {'employee_id': '3', 'project_id': '2', 'role': 'Tester'},
        {'employee_id': '4', 'project_id': '2', 'role': 'Team Lead'},
        {'employee_id': '11', 'project_id': '2', 'role': 'UI/UX Designer'},
        {'employee_id': '21', 'project_id': '2', 'role': 'Developer'}
    ]

    res = database.select("employees_projects", employee_id=1)
    assert len(res) == 2
    assert res == [data[0], data[4]]

    res = database.select("employees_projects", employee_id=4)
    assert len(res) == 1
    assert res[0] == data[-3]

    res = database.select("employees_projects", employee_id=5)
    assert len(res) == 0

    res = database.select("employees_projects", project_id=1)
    assert len(res) == 4
    assert res == data[:4]

    res = database.select("employees_projects", project_id=2)
    assert len(res) == 5
    assert res == data[4:]

    res = database.select("employees_projects", project_id=3)
    assert len(res) == 0

    res = database.select("employees_projects", employee_id=10, project_id=1)
    assert len(res) == 1
    assert res[0] == data[2]

    res = database.select("employees_projects", employee_id=1, project_id=2)
    assert len(res) == 1
    assert res[0] == data[4]

    res = database.select("employees_projects", employee_id=1, project_id=3)
    assert len(res) == 0

def test_join_employees_departments(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")
    database.insert("employees", "3 Alice 29 45000 3")
    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("departments", "3 Engineering")

    data = [
        {
            'employees.id': '1', 'employees.name': 'John',
            'employees.age': '28', 'employees.salary': '50000',
            'employees.department_id': '1', 'departments.id': '1',
            'departments.department_name': 'HR'
        },
        {
            'employees.id': '2', 'employees.name': 'Jane',
            'employees.age': '34', 'employees.salary': '60000',
            'employees.department_id': '2', 'departments.id': '2',
            'departments.department_name': 'Finance'
        },
        {
            'employees.id': '3', 'employees.name': 'Alice',
            'employees.age': '29', 'employees.salary': '45000',
            'employees.department_id': '3', 'departments.id': '3',
            'departments.department_name': 'Engineering'
        }
    ]

    res = database.join(
        tables=("employees", "departments"),
        join_attrs=[("employees.department_id", "departments.id")]
    )

    assert len(res) == 3
    assert res == data

def test_join_employees_employees_projects_projects(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")
    database.insert("employees_projects", "1,1,Developer")
    database.insert("employees_projects", "2,1,Project Manager")
    database.insert("employees_projects", "1,2,Developer")
    database.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")
    database.insert("projects", "2,CRM Development,2024-02-01,2024-08-01")

    data = [
        {
            'employees.id': '1', 'employees.name': 'John',
            'employees.age': '28', 'employees.salary': '50000',
            'employees.department_id': '1', 'employees_projects.employee_id': '1',
            'employees_projects.project_id': '1', 'employees_projects.role': 'Developer',
            'projects.id': '1', 'projects.name': 'Website Redesign',
            'projects.start_date': '2024-01-15', 'projects.end_date': '2024-03-15'
        },
        {
            'employees.id': '1', 'employees.name': 'John',
            'employees.age': '28', 'employees.salary': '50000',
            'employees.department_id': '1', 'employees_projects.employee_id': '1',
            'employees_projects.project_id': '2', 'employees_projects.role': 'Developer',
            'projects.id': '2', 'projects.name': 'CRM Development',
            'projects.start_date': '2024-02-01', 'projects.end_date': '2024-08-01'
        },
        {
            'employees.id': '2', 'employees.name': 'Jane',
            'employees.age': '34', 'employees.salary': '60000',
            'employees.department_id': '2', 'employees_projects.employee_id': '2',
            'employees_projects.project_id': '1', 'employees_projects.role': 'Project Manager',
            'projects.id': '1', 'projects.name': 'Website Redesign',
            'projects.start_date': '2024-01-15', 'projects.end_date': '2024-03-15'
        }
    ]

    res = database.join(
        tables=("employees", "employees_projects", "projects"),
        join_attrs=[
            ("employees.id", "employees_projects.employee_id"),
            ("employees_projects.project_id", "projects.id")
        ]
    )

    assert len(res) == 3
    assert res == data

def test_join_employees_departments_employees_projects_projects(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")
    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("employees_projects", "1,1,Developer")
    database.insert("employees_projects", "2,1,Project Manager")
    database.insert("employees_projects", "1,2,Developer")
    database.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")
    database.insert("projects", "2,CRM Development,2024-02-01,2024-08-01")

    data = [
        {
            'employees.id': '1', 'employees.name': 'John',
            'employees.age': '28', 'employees.salary': '50000',
            'employees.department_id': '1', 'departments.id': '1',
            'departments.department_name': 'HR', 'employees_projects.employee_id': '1',
            'employees_projects.project_id': '1', 'employees_projects.role': 'Developer',
            'projects.id': '1', 'projects.name': 'Website Redesign',
            'projects.start_date': '2024-01-15', 'projects.end_date': '2024-03-15'
        },
        {
            'employees.id': '1', 'employees.name': 'John',
            'employees.age': '28', 'employees.salary': '50000',
            'employees.department_id': '1', 'departments.id': '1',
            'departments.department_name': 'HR', 'employees_projects.employee_id': '1',
            'employees_projects.project_id': '2', 'employees_projects.role': 'Developer',
            'projects.id': '2', 'projects.name': 'CRM Development',
            'projects.start_date': '2024-02-01', 'projects.end_date': '2024-08-01'
        },
        {
            'employees.id': '2', 'employees.name': 'Jane',
            'employees.age': '34', 'employees.salary': '60000',
            'employees.department_id': '2', 'departments.id': '2',
            'departments.department_name': 'Finance', 'employees_projects.employee_id': '2',
            'employees_projects.project_id': '1', 'employees_projects.role': 'Project Manager',
            'projects.id': '1', 'projects.name': 'Website Redesign',
            'projects.start_date': '2024-01-15', 'projects.end_date': '2024-03-15'
        }
    ]

    res = database.join(
        tables=("employees", "departments", "employees_projects", "projects"),
        join_attrs=[
            ("employees.department_id", "departments.id"),
            ("employees.id", "employees_projects.employee_id"),
            ("employees_projects.project_id", "projects.id")
        ]
    )

    assert len(res) == 3
    assert res == data

def test_aggregate_method_of_the_database(database):
    database.insert("employees", "1 John 28 50000 1")
    database.insert("employees", "2 Jane 34 60000 2")
    database.insert("employees", "3 Alice 29 45000 3")
    database.insert("employees", "4 Bob 40 70000 4")
    database.insert("employees", "5 Charlie 25 40000 5")
    database.insert("employees", "6 David 30 55000 6")
    database.insert("employees", "7 Emily 38 65000 7")
    database.insert("employees", "8 Frank 50 80000 8")
    database.insert("employees", "9 Grace 33 47000 9")
    database.insert("employees", "10 Helen 29 53000 10")
    database.insert("employees", "11 Ivan 26 48000 11")
    database.insert("employees", "12 Jack 45 72000 12")
    database.insert("employees", "13 Kathy 32 55000 13")
    database.insert("employees", "14 Leo 28 49000 14")
    database.insert("employees", "15 Mona 31 60000 15")
    database.insert("employees", "16 Nathan 37 67000 16")
    database.insert("employees", "17 Olivia 29 52000 17")
    database.insert("employees", "18 Paul 34 59000 18")
    database.insert("employees", "19 Quincy 41 75000 19")
    database.insert("employees", "20 Rachel 33 54000 20")
    database.insert("employees", "21 Sam 27 46000 21")
    database.insert("employees", "22 Tina 39 71000 22")
    database.insert("employees", "23 Ursula 36 63000 23")
    database.insert("employees", "24 Victor 42 77000 24")
    database.insert("employees", "25 Wendy 32 68000 25")

    res = database.aggregate(
        table_name="employees",
        column="name",
        operation="COUNT"
    )
    assert res == {'COUNT(name)': '25'}

    res = database.aggregate(
        table_name="employees",
        column="salary",
        operation="MIN"
    )
    assert res == {'MIN(salary)': '40000'}

    res = database.aggregate(
        table_name="employees",
        column="salary",
        operation="MAX"
    )
    assert res == {'MAX(salary)': '80000'}

    res = database.aggregate(
        table_name="employees",
        column="salary",
        operation="SUM"
    )
    assert res == {'SUM(salary)': '1481000'}

    res = database.aggregate(
        table_name="employees",
        column="salary",
        operation="AVG"
    )
    assert res == {'AVG(salary)': '59240.0'}

    res = database.aggregate(
        table_name="employees",
        column="id",
        operation="MIN"
    )
    assert res == {'MIN(id)': '1'}

    res = database.aggregate(
        table_name="employees",
        column="id",
        operation="MAX"
    )
    assert res == {'MAX(id)': '25'}

    res = database.aggregate(
        table_name="employees",
        column="id",
        operation="SUM"
    )
    assert res == {'SUM(id)': '325'}

    res = database.aggregate(
        table_name="employees",
        column="name",
        operation="MIN"
    )
    assert res == {'MIN(name)': 'Alice'}

    res = database.aggregate(
        table_name="employees",
        column="name",
        operation="MAX"
    )
    assert res == {'MAX(name)': 'Wendy'}

    with pytest.raises(TypeError):
        database.aggregate(
            table_name="employees",
            column="name",
            operation="AVG"
        )

    with pytest.raises(TypeError):
        database.aggregate(
            table_name="employees",
            column="name",
            operation="SUM"
        )

def test_aggregate_method_with_group_by_of_the_database(database):
    database.insert("employees_projects", "1,1,Developer")
    database.insert("employees_projects", "2,1,Project Manager")
    database.insert("employees_projects", "10,1,Consultant")
    database.insert("employees_projects", "20,1,Customer Success Manager")
    database.insert("employees_projects", "1,2,Developer")
    database.insert("employees_projects", "3,2,Tester")
    database.insert("employees_projects", "4,2,Team Lead")
    database.insert("employees_projects", "11,2,UI/UX Designer")
    database.insert("employees_projects", "21,2,Developer")
    database.insert("employees_projects", "2,3,Project Manager")
    database.insert("employees_projects", "5,3,Business Analyst")
    database.insert("employees_projects", "6,3,Developer")
    database.insert("employees_projects", "12,3,Backend Developer")
    database.insert("employees_projects", "22,3,Project Manager")
    database.insert("employees_projects", "3,4,Tester")
    database.insert("employees_projects", "7,4,Marketing Specialist")
    database.insert("employees_projects", "8,4,Content Creator")
    database.insert("employees_projects", "13,4,Database Administrator")
    database.insert("employees_projects", "23,4,Tester")
    database.insert("employees_projects", "4,5,Team Lead")
    database.insert("employees_projects", "9,5,Financial Analyst")
    database.insert("employees_projects", "10,5,Consultant")
    database.insert("employees_projects", "14,5,System Architect")
    database.insert("employees_projects", "24,5,Team Lead")
    database.insert("employees_projects", "5,6,Business Analyst")
    database.insert("employees_projects", "11,6,UI/UX Designer")
    database.insert("employees_projects", "12,6,Backend Developer")
    database.insert("employees_projects", "15,6,Wiki Maintainer")
    database.insert("employees_projects", "25,6,Business Analyst")
    database.insert("employees_projects", "6,7,Developer")
    database.insert("employees_projects", "13,7,Database Administrator")
    database.insert("employees_projects", "14,7,System Architect")
    database.insert("employees_projects", "16,7,Technical Writer")
    database.insert("employees_projects", "7,8,Marketing Specialist")
    database.insert("employees_projects", "15,8,Wiki Maintainer")
    database.insert("employees_projects", "16,8,Technical Writer")
    database.insert("employees_projects", "17,8,Cloud Engineer")
    database.insert("employees_projects", "8,9,Content Creator")
    database.insert("employees_projects", "17,9,Cloud Engineer")
    database.insert("employees_projects", "18,9,DevOps Specialist")
    database.insert("employees_projects", "19,9,Data Analyst")
    database.insert("employees_projects", "9,10,Financial Analyst")
    database.insert("employees_projects", "18,10,DevOps Specialist")
    database.insert("employees_projects", "19,10,Data Analyst")
    database.insert("employees_projects", "20,10,Customer Success Manager")

    data = [
        {'project_id': '1', 'COUNT(employee_id)': '4'},
        {'project_id': '2', 'COUNT(employee_id)': '5'},
        {'project_id': '3', 'COUNT(employee_id)': '5'},
        {'project_id': '4', 'COUNT(employee_id)': '5'},
        {'project_id': '5', 'COUNT(employee_id)': '5'},
        {'project_id': '6', 'COUNT(employee_id)': '5'},
        {'project_id': '7', 'COUNT(employee_id)': '4'},
        {'project_id': '8', 'COUNT(employee_id)': '4'},
        {'project_id': '9', 'COUNT(employee_id)': '4'},
        {'project_id': '10', 'COUNT(employee_id)': '4'},
    ]

    res = database.aggregate(
        table_name="employees_projects",
        column="employee_id",
        operation="COUNT",
        group_by="project_id"
    )
    assert res == data
