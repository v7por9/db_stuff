import pymysql


class Active:
    def __init__(self):
        self.password = input("Enter database password: ")
        self.con = pymysql.connect(host='localhost', user='root', port=3306, passwd=self.password)
        self.cur = self.con.cursor()
        return

    def db_action(self, db_entry):
        """This is to execute MySQL Commands into the Connector.
        :returns: Nothing..."""
        self.cur.execute(db_entry)
        return

    def db_showing(self):
        """
        With calling this, calling `show databases`
        :return: All the databases that have been created.
        """
        self.db_action("show databases;")
        return self.cur.fetchall()

    def tb_display(self, which_db):
        """
        :param which_db: Which stating all the db.
        :return: all the tables that are in "which_db"
        """
        self.db_action('use {}' .format(which_db))
        self.db_action("show tables;")
        return self.cur.fetchall()

    def tb_describe(self, which_db):
        print(eval(self.tb_display(which_db)))
        exit()
        self.db_action("use %s" % which_db)
        self.db_action("describe %s" % self.tb_display(which_db))
        return self.cur.fetchall()


a = Active()
print(a.db_showing())
print(a.tb_display('ptr_database'))
print(a.tb_describe("ptr_database"))