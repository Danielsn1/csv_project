import datetime
import mysql.connector
import csv
import sshtunnel
import paramiko

# ssh = paramiko.SSHClient()
# ssh.load_system_host_keys()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
# ssh.connect("domain", username='username', password='password', look_for_keys=False)


DB_NAME = 'nd23_csv_test'

# cnx = mysql.connector.connect(username='username', password='password', ssl_disabled=True)
cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', port='3306')

# sshtunnel.SSH_TIMEOUT = 5.0
# sshtunnel.TUNNEL_TIMEOUT = 5.0
#
# with sshtunnel.SSHTunnelForwarder(
#     ('domain'),
#     ssh_username='username', ssh_password='password',
#     remote_bind_address=('127.0.0.1', 3306)
# ) as tunnel:
#     print(tunnel.tunnel_is_up,tunnel.tunnel_bindings,tunnel.is_active,tunnel.get_agent_keys(tunnel.logger))
#     cnx = mysql.connector.connect(
#         user='username', password='password',
#         host='127.0.0.1', port=3306
#     )
#     cursor = cnx.cursor()
#     print(cursor)

TABLES = {'classes': (
    "CREATE TABLE classes("
    "   institution VARCHAR(10),"
    "   course_number VARCHAR(20),"
    "   launch_date DATE,"
    "   course_title VARCHAR(120),"
    "   instructors VARCHAR(150),"
    "   course_subject VARCHAR(100),"
    "   year INTEGER,"
    "   honor_code_certificates INTEGER,"
    "   participants INTEGER,"
    "   complete_50_percent INTEGER,"
    "   certified INTEGER,"
    "   percent_audited DOUBLE,"
    "   percent_certified DOUBLE,"
    "   percent_certified_over_50_percent_done DOUBLE,"
    "   percent_played_video DOUBLE,"
    "   percent_posted_in_forum DOUBLE,"
    "   percent_grade_higher_than_zero DOUBLE,"
    "   course_hours_per_1000_students DOUBle,"
    "   median_hours_certification DOUBLE,"
    "   median_age INTEGER,"
    "   percent_male DOUBLE,"
    "   percent_female DOUBLE,"
    "   percent_bachelors_degree DOUBLE)")}

cursor = cnx.cursor()

cursor.execute("DROP DATABASE IF EXISTS " + DB_NAME)
cursor.execute("CREATE DATABASE " + DB_NAME)
cursor.execute("USE " + DB_NAME)

cnx.database = DB_NAME

for table_name in TABLES:
    table_description = TABLES[table_name]
    cursor.execute(table_description)

with open('C:\\Users\\13194\\Desktop\\appendix.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
    header = next(csv_reader)
    data = ""
    for row in csv_reader:
        data = "INSERT INTO classes " \
               "VALUE" \
               "("
        for elements in range(len(row)):
            if row[elements] == "---":
                row[elements] = "0"
            if elements == len(row) - 1:
                data += "%s)"
            elif elements == 2:
                dates = row[elements].split("/")
                row[elements] = datetime.date(int(dates[2]), int(dates[0]), int(dates[1]))
                data += "%s, "
            else:
                data += "%s, "
        values = tuple(row)
        cursor.execute(data, values)

cnx.commit()

cursor.close()
cnx.close()

cnx.connect(host='127.0.0.1', user='root', password='password', port='3306')
cursor = cnx.cursor()

cursor.execute("USE " + DB_NAME)

query = "SELECT * FROM classes"

cursor.execute(query)

count = 0
for rows in cursor:
    print(rows)
    count += 1
print(count)
