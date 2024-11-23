from database.database import (
    Database, EmployeeTable, DepartmentTable,
    ProjectTable, EmployeeProjectTable
)


if __name__ == "__main__" :
    db = Database()

    # Создание таблиц в базе данных
    db.register_table("employees", EmployeeTable())
    db.register_table("departments", DepartmentTable())
    db.register_table("projects", ProjectTable())
    db.register_table("employees_projects", EmployeeProjectTable())

    # Вставка элементов
    db.insert("employees", "1 John 28 50000 1")
    db.insert("employees", "2 Jane 34 60000 2")
    db.insert("employees", "3 Alice 29 45000 3")
    db.insert("employees", "4 Bob 40 70000 4")
    db.insert("employees", "5 Charlie 25 40000 5")
    db.insert("employees", "6 David 30 55000 6")
    db.insert("employees", "7 Emily 38 65000 7")
    db.insert("employees", "8 Frank 50 80000 8")
    db.insert("employees", "9 Grace 33 47000 9")
    db.insert("employees", "10 Helen 29 53000 10")
    db.insert("employees", "11 Ivan 26 48000 11")
    db.insert("employees", "12 Jack 45 72000 12")
    db.insert("employees", "13 Kathy 32 55000 13")
    db.insert("employees", "14 Leo 28 49000 14")
    db.insert("employees", "15 Mona 31 60000 15")
    db.insert("employees", "16 Nathan 37 67000 16")
    db.insert("employees", "17 Olivia 29 52000 17")
    db.insert("employees", "18 Paul 34 59000 18")
    db.insert("employees", "19 Quincy 41 75000 19")
    db.insert("employees", "20 Rachel 33 54000 20")
    db.insert("employees", "21 Sam 27 46000 21")
    db.insert("employees", "22 Tina 39 71000 22")
    db.insert("employees", "23 Ursula 36 63000 23")
    db.insert("employees", "24 Victor 42 77000 24")
    db.insert("employees", "25 Wendy 32 68000 25")

    db.insert("departments", "1 HR")
    db.insert("departments", "2 Finance")
    db.insert("departments", "3 Engineering")
    db.insert("departments", "4 Marketing")
    db.insert("departments", "5 Sales")
    db.insert("departments", "6 IT Support")
    db.insert("departments", "7 Product Management")
    db.insert("departments", "8 Customer Service")
    db.insert("departments", "9 Legal")
    db.insert("departments", "10 R&D")
    db.insert("departments", "11 Operations")
    db.insert("departments", "12 Business Development")
    db.insert("departments", "13 Accounting")
    db.insert("departments", "14 Training & Development")
    db.insert("departments", "15 Supply Chain")
    db.insert("departments", "16 Logistics")
    db.insert("departments", "17 Public Relations")
    db.insert("departments", "18 Compliance")
    db.insert("departments", "19 Quality Assurance")
    db.insert("departments", "20 Procurement")
    db.insert("departments", "21 Security")
    db.insert("departments", "22 Data Science")
    db.insert("departments", "23 Design")
    db.insert("departments", "24 Admin")
    db.insert("departments", "25 Corporate Strategy")

    db.insert("projects", "1,Website Redesign,2024-01-15,2024-03-15")
    db.insert("projects", "2,CRM Development,2024-02-01,2024-08-01")
    db.insert("projects", "3,HR Automation,2024-01-20,2024-04-20")
    db.insert("projects", "4,Marketing Campaign,2024-03-10,2024-06-10")
    db.insert("projects", "5,Financial Report Tool,2024-02-15,2024-05-15")
    db.insert("projects", "6,Mobile App Development,2024-04-01,2024-09-01")
    db.insert("projects", "7,Data Migration,2024-05-01,2024-07-31")
    db.insert("projects", "8,Internal Wiki,2024-02-05,2024-04-05")
    db.insert("projects", "9,Cloud Infrastructure Setup,2024-06-01,2024-12-01")
    db.insert("projects", "10,Customer Feedback Analysis,2024-07-01,2024-09-01")

    db.insert("employees_projects", "1,1,Developer")
    db.insert("employees_projects", "2,1,Project Manager")
    db.insert("employees_projects", "10,1,Consultant")
    db.insert("employees_projects", "20,1,Customer Success Manager")
    db.insert("employees_projects", "1,2,Developer")
    db.insert("employees_projects", "3,2,Tester")
    db.insert("employees_projects", "4,2,Team Lead")
    db.insert("employees_projects", "11,2,UI/UX Designer")
    db.insert("employees_projects", "21,2,Developer")
    db.insert("employees_projects", "2,3,Project Manager")
    db.insert("employees_projects", "5,3,Business Analyst")
    db.insert("employees_projects", "6,3,Developer")
    db.insert("employees_projects", "12,3,Backend Developer")
    db.insert("employees_projects", "22,3,Project Manager")
    db.insert("employees_projects", "3,4,Tester")
    db.insert("employees_projects", "7,4,Marketing Specialist")
    db.insert("employees_projects", "8,4,Content Creator")
    db.insert("employees_projects", "13,4,Database Administrator")
    db.insert("employees_projects", "23,4,Tester")
    db.insert("employees_projects", "4,5,Team Lead")
    db.insert("employees_projects", "9,5,Financial Analyst")
    db.insert("employees_projects", "10,5,Consultant")
    db.insert("employees_projects", "14,5,System Architect")
    db.insert("employees_projects", "24,5,Team Lead")
    db.insert("employees_projects", "5,6,Business Analyst")
    db.insert("employees_projects", "11,6,UI/UX Designer")
    db.insert("employees_projects", "12,6,Backend Developer")
    db.insert("employees_projects", "15,6,Wiki Maintainer")
    db.insert("employees_projects", "25,6,Business Analyst")
    db.insert("employees_projects", "6,7,Developer")
    db.insert("employees_projects", "13,7,Database Administrator")
    db.insert("employees_projects", "14,7,System Architect")
    db.insert("employees_projects", "16,7,Technical Writer")
    db.insert("employees_projects", "7,8,Marketing Specialist")
    db.insert("employees_projects", "15,8,Wiki Maintainer")
    db.insert("employees_projects", "16,8,Technical Writer")
    db.insert("employees_projects", "17,8,Cloud Engineer")
    db.insert("employees_projects", "8,9,Content Creator")
    db.insert("employees_projects", "17,9,Cloud Engineer")
    db.insert("employees_projects", "18,9,DevOps Specialist")
    db.insert("employees_projects", "19,9,Data Analyst")
    db.insert("employees_projects", "9,10,Financial Analyst")
    db.insert("employees_projects", "18,10,DevOps Specialist")
    db.insert("employees_projects", "19,10,Data Analyst")
    db.insert("employees_projects", "20,10,Customer Success Manager")
