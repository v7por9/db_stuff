#! /usr/bin/python3

import pymysql
import os
import time
import sys
import hashlib
import io
from math import log


class Filling:
    def __init__(self):
        self.path = '/mnt/ice' #input("Enter path:")
        self.gen = []
        try:
            for folder, sub, files in os.walk(self.path):
                for names in os.listdir(folder):
                    if os.path.isfile(os.path.join(folder, names)):
                        self.gen.append(os.path.join(folder, names))
        except EnvironmentError:
            pass


class Project:
        def __init__(self):
            # Details of logging into the server.
            """self.host, self.user, self.password, self.port = input("Enter mysql host IP: "), \
                                                             input("Enter username: "),\
                                                             input("Enter password: "), input("Enter port: ")

            if self.host == '' or self.user == '' or self.port == '':"""
            self.host = 'localhost'
            self.user = 'root'
            self.port = 3306
            self.password = 'storage'
            self.table_set = time.asctime().split(" ")
            self.time_name = self.table_set[-2].split(":")
            self.db_name = self.table_set[-1] + "_" + self.table_set[1] + "_" + \
                           self.time_name[0] + self.time_name[1] + "_Bckp"
            self.tb_name = str(self.table_set[1] + "_" + self.table_set[2] + "_" + self.table_set[-1] +
                               "_" + self.time_name[0] + self.time_name[1])

            # Connecting to mysql server using pymysql module
            # TODO: Add module pymysql as a requirement for running the script.
            try:
                # This block only tests the connection to the server and does not set variables
                self.connection = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password)
                self.cursor = self.connection.cursor()
            except pymysql.err.OperationalError as error:
                print(''' \n"1045, "Access denied for user 'root'@'localhost' (using password: NO)"''')

            # NOTE NOT REPETITION...
            # TODO : Nest the error much better... and eliminate this.
            # Setting the variables required.
            self.connection = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password)
            self.cursor = self.connection.cursor()

        def action(self, query):
            """
            :param query: Adding mysql Query into the script.
            :return: a fetch of the query if required.
            """
            self.execute = self.cursor.execute(query)
            return self.cursor.fetchall()

        def creating_DB(self):
            self.action("create database %s" % self.db_name)

        def single_DB(self):
            """Searching for the database to use in order to only have a single DB and create multiple tables"""
            db_using = ""
            db_all = eval(str("[" + (str(self.action('show schemas')).replace('(', '').replace(",)", "").replace(')', "")) + "]"))
            for x in db_all:
                if "Bckp" in x.split("_"):
                    db_using += str(x)
            return db_using

        def tb_Create(self):
            # TODO; add an exception if the table already exists to rename + 1
            self.action('use %s' %self.single_DB())
            self.action("""create table %s (
                        id int(12) not null primary key auto_increment,
                        filename text,
                        size text, hash_key text);""" % self.tb_name)

        def size_file(self, bites, pow=0, b=1024, u='B', pre=[''] + [p + 'i' for p in 'KMGTPEZY']):
            pow, n = min(int(log(max(bites * b ** pow, 1), b)), len(pre) - 1), bites * b ** pow
            return "%%.%if %%s%%s" % abs(pow % (-pow - 1)) % (bites / b ** float(pow), pre[pow], u)

        def enter_Data(self, values, size, key_hash):
            self.cursor.execute("""insert into %s (filename, size, hash_key) values ("%s", '%s', '%s');""" %
                                (self.tb_name, values, size, key_hash))
            return

        def commit(self):
            self.cursor.close()
            self.connection.commit()


# Initialing the code block.
initial = Filling()
archive = Project()

# Confirming the existence of another db and if exists creating a Table within it.
if archive.single_DB() != "":
    archive.tb_Create()
    print("Database already exists... Using %s" % str(archive.single_DB()))

elif archive.single_DB() == "":
    archive.creating_DB()
    archive.tb_Create()
    print("Database already exists... Using %s" % str(archive.single_DB()))

# Entering the data into the DB.
for filenames in initial.gen:
    #Unorderly Hashing of files
    key_hash = hashlib.sha256(io.FileIO(filenames).read(10240)).hexdigest()
    archive.enter_Data(filenames, archive.size_file(os.path.getsize(filenames)), key_hash)
archive.commit()
