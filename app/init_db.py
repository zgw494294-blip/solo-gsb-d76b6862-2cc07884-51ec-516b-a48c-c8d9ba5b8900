"""独立的数据库初始化入口：建表并清理残留暂存表。"""
from .db import init_db

if __name__ == "__main__":
    init_db()
    print("database initialized")
