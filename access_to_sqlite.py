import sqlite3
import logging



class SqliteAccess():
    def __init__(self, dbname):
        self.dbname = dbname
        self.connection = None
        self.cursor=None
        logging.basicConfig(format='%(asctime)s: %(levelname)s: %(funcName)s:%(lineno)d: %(message)s',
                            level=logging.INFO, handlers=[logging.FileHandler("program_log.log"), logging.StreamHandler()])

    def open_connection(self):
        # open and if not exxst create and open
        try:
            self.connection = sqlite3.connect(self.dbname)
            self.cursor=self.connection.cursor()
            #foreign_keys = ON is necessary to do necessary action after deleting/updating a row containing a referenced foreign key. 
            self.execute_sql("PRAGMA foreign_keys = ON")
            
            logging.info(f"connection establisted to {self.dbname}")
        except sqlite3.Error:
            logging.error(f"connection failure to {self.dbname}")

 
    def execute_script_sql(self, sql_command: str):
        """this method prevents sql injection for sqlite"""
        try:
            #create a cursor, a transaction and commit due to with context manager   
            self.cursor = self.connection.cursor()
            self.cursor = self.cursor.executescript(sql_command)
            self.connection.commit()
            logging.info(f"execution sucessfull: {sql_command}")
        except sqlite3.Error:
            logging.error(f"execution failure: {sql_command}")
        return self.cursor
        
    def execute_sql(self, sql_command: str, fields=None):
        """this method prevents sql injection for sqlite"""
        try:
            #create a cursor, a transaction and commit due to with context manager   
            self.cursor = self.connection.cursor()
            if fields:
                self.cursor=self.cursor.execute(sql_command,fields)
            else:
                self.cursor=self.cursor.execute(sql_command)
            self.connection.commit()

            logging.info(f"execution sucessfull: {sql_command}")
        except sqlite3.Error:
            logging.error(f"execution failure: {sql_command}")

        return self.cursor

    def close_connection(self):
        try:
            self.connection.close()
            self.connection=None
            logging.info(f"close connection successfull to {self.dbname}")
        except sqlite3.Error:
            logging.warning(f"close connection to failure to {self.dbname}")

if __name__=="__main__":

    command1="""CREATE TABLE IF NOT EXISTS entries(content TEXT, date TEXT);"""
    command2 = """CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY,
        first_name TEXT, 
        surname TEXT, 
        age INTEGER
        );"""
    command3= "INSERT INTO users (first_name, surname,age) VALUES ('Rolf','Smith',35);" 
    command4 = "INSERT INTO users  VALUES (NULL,'John','Snow','19');" #NULL for primary key field.

    #secure command against sql injection:
    command5="INSERT INTO users VALUES (NULL,?,?,?);" #NULL for primary key field.
    command5_tuple=('Bla','Blo','55')

    command6="SELECT * FROM users;"
    command7="SELECT * FROM users WHERE age > 19;"
    command8 = "SELECT * FROM users WHERE first_name ='John';"
    command9 = "SELECT * FROM users WHERE first_name !='John';"
    command10 = "SELECT * FROM users WHERE first_name !='John' AND age > 19;"

    #secure command against sql injection:
    command11 = "SELECT * FROM users WHERE first_name = ?;"
    command11_tuple = ('John',)

    command12="DROP TABLE IF EXISTS users;"
    command13="CREATE INDEX IF NOT EXISTS idx_age  ON users(age);" #create an index for quick searches
    command14="DROP INDEX IF EXISTS idx_age;"
    
    command0 = """
                CREATE TABLE suppliers (
                supplier_id   INTEGER PRIMARY KEY,
                supplier_name TEXT    NOT NULL,
                group_id    INTEGER,
                FOREIGN KEY (group_id)
                REFERENCES supplier_groups (group_id) 
                    ON UPDATE SET NULL
                    ON DELETE SET NULL
            );

            CREATE TABLE supplier_groups (
                group_id integer PRIMARY KEY,
                group_name text NOT NULL
            );

            INSERT INTO supplier_groups (group_name)
            VALUES
            ('Domestic'),
            ('Global'),
            ('One-Time');

            INSERT INTO suppliers (supplier_name, group_id) VALUES ('HP', 2);
            INSERT INTO suppliers (supplier_name, group_id) VALUES('XYZ Corp', 2);
            INSERT INTO suppliers (supplier_name, group_id) VALUES('ABC Corp', 3);

            """
    #demonstration to show that when group_id = 3 record in suplier_groups deleted group_id field of supplier is automatically set to null because of  "ON DELETE SET NULL" in table creation

    command000 = "DELETE FROM supplier_groups WHERE group_id = 3;"
    commandjoin ="""
            SELECT supplier_name,supplier_groups.group_name 
            FROM suppliers
            INNER JOIN supplier_groups
            ON suppliers.group_id = supplier_groups.group_id;
            """
    #view is a query object stored in the database and can be used later on.
    commandcreateview = """
            CREATE VIEW suplquery
            AS 
            SELECT supplier_name,supplier_groups.group_name 
            FROM suppliers
            INNER JOIN supplier_groups
            ON suppliers.group_id = supplier_groups.group_id;
            """
    commandcallview = "SELECT * FROM suplquery;"

    sq = SqliteAccess("data.db")
    sq.open_connection()

    sq.execute_sql(command1)
    sq.execute_sql(command2)
    sq.execute_sql(command13)
    sq.execute_sql(command3)
    sq.execute_sql(command4)
    sq.execute_sql(command5, command5_tuple)
    rows=sq.execute_sql(command10)
    for row in rows:
        print(row)
    rows = sq.execute_sql(command11, command11_tuple)
    for row in rows:
        print(row)

    sq.execute_sql(command13)
    sq.execute_script_sql(command0)
    sq.close_connection()

    sq.open_connection()
    rows = sq.execute_sql(commandjoin)
    for row in rows:
        print(row)
    sq.close_connection()
    
    sq.open_connection()
    sq.execute_sql(command000)
    sq.close_connection()

    sq.open_connection()
    sq.execute_sql(commandcreateview)
    rows=sq.execute_sql(commandcallview)
    for row in rows:
        print(row)
    sq.close_connection()


