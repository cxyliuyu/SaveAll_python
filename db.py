import MySQLdb
from config import DB

def getConnection():
    conn = MySQLdb.connect(host=DB["host"],user=DB["user"],passwd=DB["passwd"],db=DB["db"],charset=DB["charset"])
    return conn

def saveAll(table,datas,searchKeys,ifIgnoreSearchKey,ifNotUpdate):
    conn = getConnection()
    cursor = conn.cursor()
    where = []
    #转义数据,避免sql发生错误
    for data in datas:
        for key in data:
            data[key] = MySQLdb.escape_string(str(data[key]))
    for searchKey in searchKeys:
        searchKeyDatas = []
        for data in datas:
            searchKeyDatas.append(data[searchKey])
        searchKeyDatasString = "`"+searchKey+"` in ('"+"','".join(searchKeyDatas)+"')"
        where.append(searchKeyDatasString)
    whereString = " AND ".join(where)
    selectSql = "SELECT `"+"`,`".join(searchKeys)+"` from "+table+" WHERE " + whereString
    cursor.execute(selectSql)
    conn.commit()
    results = cursor.fetchall()
    updateData = []
    insertData = []
    existKey = []
    for result in results:
        keyValue = []
        for value in result:
            keyValue.append(str(value))
        existKey.append(",".join(keyValue))
    for data in datas:
        keyValue = []
        for key in searchKeys:
            keyValue.append(data[key])
        currentKey = ",".join(keyValue)
        if currentKey in existKey:
            updateData.append(data)
        else:
            insertData.append(data)
    if ifNotUpdate == "0":
        updateAll(updateData,table,searchKeys)
    insertAll(insertData,table,searchKeys,ifIgnoreSearchKey)
    conn.close()
    pass

def updateAll(datas,table,searchKeys):
    #同时更新多条数据
    if len(datas) == 0:
        return
    conn = getConnection()
    cursor = conn.cursor()
    sets = {}
    updateSql = "UPDATE `"+table+"` set "
    whereValues = []
    whereKey = "WHERE CONCAT(`"+"`,',',`".join(searchKeys)+"`) IN "
    for data in datas:
        whereValue = []
        for searchKey in searchKeys:
            whereValue.append(data[searchKey])
        whereValueString = ",".join(whereValue)
        whereValues.append(whereValueString)
        for key in data:
            if key in searchKeys:
                pass
            else:
                searchValue = []
                for searchKey in searchKeys:
                    searchValue.append(str(data[searchKey]))
                searchValueString = ",".join(searchValue)
                try:
                    sets[key][searchValueString] = data[key]
                except KeyError as e:
                    sets[key] = {}
                    sets[key][searchValueString] = data[key]
    searchKeysString = "(`"+"`,',',`".join(searchKeys)+"`)"
    whereValuesString = "('"+"','".join(whereValues)+"')"
    setStringArray = []
    for key1 in sets:
        setString = ""
        for key2 in sets[key1]:
            if setString == "":
                setString = "`"+key1+"` = CASE WHEN (CONCAT"+searchKeysString+"='"+key2+"') THEN '"+sets[key1][key2]+"'"
            else:
                setString = setString + " WHEN (CONCAT"+searchKeysString+"='"+key2+"') THEN '"+sets[key1][key2]+"'"
        setString += " END "
        setStringArray.append(setString)
    setStrings = ",".join(setStringArray)
    whereStrings = whereKey + whereValuesString
    updateSql += setStrings
    updateSql += whereStrings
    try:
        cursor.execute(updateSql)
        result = cursor.fetchall()
    except Exception as e:
        print e.message
        print updateSql
    conn.commit()
    conn.close()

def insertAll(datas,table,searchKeys,ifIgnoreSearchKey):
    #多条数据同时添加
    if len(datas) == 0:
        return
    conn = getConnection()
    cursor = conn.cursor()
    keys=[]
    for key in datas[0]:
        if key not in searchKeys or ifIgnoreSearchKey!= "1":
            keys.append(key)
    insertSql = "INSERT INTO "+table+" (`"+"`,`".join(keys)+"`) VALUES "
    valueStrings = []
    for data in datas:
        value = []
        for key in keys:
            value.append(data[key])
        valueString = "('" + "','".join(value) + "')"
        valueStrings.append(valueString)
    insertSql += ",".join(valueStrings)
    try:
        cursor.execute(insertSql)
        conn.commit()
        conn.close()
    except Exception as e:
        print insertSql
        print e.message
