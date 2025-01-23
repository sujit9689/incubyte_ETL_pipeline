import mysql.connector
import os

MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


config = {
"host": "127.0.0.1",
"port": 3306,
"user": MYSQL_USER,
"password": MYSQL_PASSWORD,
"use_pure": True
}

def connect():
    return mysql.connector.connect(**config)