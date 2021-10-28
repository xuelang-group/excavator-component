from suanpan.app.modules.base import Module
from mysql import connector
from utils import common
import subprocess
import random


module = Module()

@module.on("data.test")
def test(context):
    print(context)
    return {"res":"success"}

@module.on("data.get")
def sendData(context):
    try:
        config = context["args"]

        username = subprocess.check_output('wmic csproduct get uuid').split(b'\n')[1].strip().decode("utf-8")
        sqlTopTen = "select * from `top`;"
        sqlPlayerRecord = f"select user_name,sum(ore_1),sum(ore_2),sum(ore_1),sum(ore_1) from `player_record` group by `user_name` having `user_name`='{username}';"

        with common.getConnection() as connection:
            cursor = connection.cursor()
            cursor.execute(sqlTopTen)
            resTop = cursor.fetchall()
            cursor.execute(sqlPlayerRecord)
            resRecord = cursor.fetchall()
            cursor.close()
        
        return common.formatJson({"top": resTop, "record": resRecord})
    except Exception as e:
        return {"error": e }

@module.on("data.update")
def sendTopData(context):
    try:
        config = context["args"]

        username = subprocess.check_output('wmic csproduct get uuid').split(b'\n')[1].strip().decode("utf-8")
        ore = [config["ore_1"], config["ore_2"], config["ore_3"], config["ore_4"], config["ore_red_packet"]]
        datetime = config["datetime"]

        sql = f"insert into `player_record` (`user_name`,`ore_1`,`ore_2`,`ore_3`,`ore_4`,`ore_red_packet`,`datetime`) values ('{username}',{ore[0]},{ore[1]},{ore[2]},{ore[3]},{ore[4]},'{datetime}');"
        with common.getConnection() as connection:
            cursor = connection.cursor()

            cursor.execute(sql)
            connection.commit()

            cursor.close()

    except Exception as e:
        return {"error": e}
    return {"result": "success"}

@module.on("data.generateRedPacket")
def generateRedPacket(context):
    try:
        location = []
        num = random.randint(0,5)
        for i in range(num):
            location.append((random.random(),random.random()))
        return common.formatJson({'result': location})
    except Exception as e:
        return {"error": e}
    

@module.on("data.signup")
def signup(context):
    try:
        config = context["args"]
        username = config["username"]
        password = config["password"]
        
        sqlCheckIfExists = f"select from `user` where `username`='{username}'"
        sqlInsertUserInfo = f"insert into `user` (`username`,`password`) values ('{username}','{password}')"
        connection = common.getConnection()
        cursor = connection.cursor()

        cursor.execute(sqlCheckIfExists)
        res = cursor.fetchall()
        if res:
            cursor.close()
            connection.close()
            return {"result": "failed"}
        else:
            cursor.execute(sqlInsertUserInfo)
            connection.commit()
            cursor.close()
            connection.close()
    except Exception as e:
        return {"error": e}
    return {"result": "success"}

@module.on("data.login")
def login(context):
    try:
        config = context["args"]
        username = config["username"]
        password = config["password"]
        
        sql = f"select from `user` where `username`='{username}' and `password`='{password}'"
        connection = common.getConnection()
        cursor = connection.cursor()

        cursor.execute(sql)
        res = cursor.fetchall()
        if not res:
            cursor.close()
            connection.close()
            return {"result": "failed"}
    except Exception as e:
        return {"error": e}
    
    cursor.close()
    connection.close()
    return {"result": "success","username":username}
