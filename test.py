import sys
from java.lang import Class
from java.sql  import DriverManager, SQLException

print "Here we go"

DRIVER_NAME = "com.amazon.redshift.jdbc.Driver"
DB_URL = "jdbc:redshift://localhost:5434/prod"
DB_USER = "etluser"
DB_PASSWORD = "*******"

#FULL_DB_URL = DB_URL + "?" + 

qry = "select ka_name, ka_code from stg_ka_data.v_key_accounts order by 1"

try:
    Class.forName(DRIVER_NAME).newInstance()
    dbConn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
    stmt = dbConn.createStatement()
    rst = stmt.executeQuery(qry)
    while rst.next():
        name = rst.getString("ka_name")
        code = rst.getString("ka_code")
        print "%-16.16s  %-8.8s" % (name, code)

    stmt.close()
    dbConn.close()
    sys.exit(0)
except Exception, msg:
    print msg
    sys.exit(-1)

