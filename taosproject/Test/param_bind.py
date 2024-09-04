"""
 @Author: CNN
 @Email: xxx@xxx.com
 @FileName: param_bind.py
 @DateTime: 2024/7/17 13:22
 @SoftWare: PyCharm
"""

import taos

conn = taos.connect(
    host="localhost",
    user="root",
    password="taosdata",
    port=6030,
)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

conn.select_db(db)
print("数据库选择完成2")

# 数据库无模式写入
lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 "
    "1626006833639000000"
]
telnetDemo = ["stb0_0 1707095283260 4 host=host0 interface=eth0"]
jsonDemo = [
    '{"metric": "meter_current","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": '
    '"California.SanFrancisco", "id": "d1001"}}'
]

conn.schemaless_insert(
    lineDemo, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS)
conn.schemaless_insert(
    telnetDemo, taos.SmlProtocol.TELNET_PROTOCOL, taos.SmlPrecision.MICRO_SECONDS)
conn.schemaless_insert(
    jsonDemo, taos.SmlProtocol.JSON_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS)

# 数据库参数绑定
# sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
# stmt = conn.statement(sql)
#
# tbname = "power.d1001"
#
# tags = taos.new_bind_params(2)
# tags[0].binary(["California.SanFrancisco"])
# tags[1].int([2])
#
# stmt.set_tbname_tags(tbname, tags)
#
# params = taos.new_bind_params(4)
# params[0].timestamp((1626861392589, 1626861392591, 1626861392592))
# params[1].float((10.3, 12.6, 12.3))
# params[2].int([194, 200, 201])
# params[3].float([0.31, 0.33, 0.31])
#
# stmt.bind_param_batch(params)
#
# stmt.execute()
#
# stmt.close()

conn.close()
