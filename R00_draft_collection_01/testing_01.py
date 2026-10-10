import pyarrow as pa
import pyarrow.dataset as ds

# 定义分区字段
partition_fields = [
    pa.field("year", pa.int32()),
    pa.field("month", pa.int32()),
    pa.field("day", pa.int32())
]

# 创建 Hive 分区对象
def hive_partitioning(fields: list[pa.Field]) -> ds.Partitioning:
    return ds.partitioning(pa.schema(fields), flavor="hive")

partitioning = hive_partitioning(partition_fields)

# 模拟数据
data = pa.table({
    "id": [1, 2, 3, 4, 5],
    "value": [10, 20, 30, 40, 50],
    "year": [2026, 2026, 2026, 2026, 2026],
    "month": [7, 7, 7, 7, 7],
    "day": [25, 25, 25, 26, 26]
})

# 1. ds.partitioning() - 创建分区对象
print("1. 创建分区对象:")
print(f"   分区模式: {partitioning.flavor}")
print(f"   分区字段: {[f.name for f in partitioning.schema]}")
print()

# 2. pa.schema() - 创建schema
schema = pa.schema(partition_fields)
print("2. 创建Schema:")
print(f"   Schema字段: {schema.names}")
print()

# 3. pa.field() - 创建字段定义
field = pa.field("date", pa.string())
print("3. 创建字段:")
print(f"   字段名: {field.name}")
print(f"   字段类型: {field.type}")
print()

# 4. pa.table() - 创建内存表
print("4. 创建内存表:")
print(f"   表行数: {len(data)}")
print(f"   表列数: {len(data.column_names)}")
print(f"   列名: {data.column_names}")
print()

# 5. 使用分区解析路径
from pyarrow import fs

# 模拟文件路径
paths = [
    "data/year=2026/month=7/day=25/file.parquet",
    "data/year=2026/month=7/day=26/file.parquet"
]

print("5. 解析Hive分区路径:")
for path in paths:
    parts = path.split('/')
    partition_values = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=')
            partition_values[key] = value
    print(f"   路径: {path}")
    print(f"   分区值: {partition_values}")
    print()

# 6. 演示数据过滤（模拟分区裁剪）
print("6. 分区裁剪示例:")
# 模拟只读取特定分区
filters = [("year", "=", 2026), ("month", "=", 7), ("day", "=", 25)]
print(f"   应用过滤器: {filters}")

# 使用dataset扫描（仅演示概念）
# 实际需要从文件系统读取
filtered_data = data.filter(
    (data["year"] == 2026) &
    (data["month"] == 7) &
    (data["day"] == 25)
)
print(f"   过滤后行数: {len(filtered_data)}")
print(f"   过滤后数据:")
print(filtered_data.to_pandas())
print()

# 7. 演示目录结构生成
print("7. 生成分区目录结构:")
from datetime import datetime

def generate_partition_path(base_path, year, month, day):
    return f"{base_path}/year={year}/month={month}/day={day}"

base = "/data/warehouse"
year, month, day = 2026, 7, 25
partition_path = generate_partition_path(base, year, month, day)
print(f"   生成的路径: {partition_path}")
print()

print("Demo完成! 所有对象均在内存中，无永久存储。")