"""Print the SQL DDL that SQLAlchemy generates for our models."""
from database.connection import engine
from database.models import Job, ScrapeLog
from database.connection import Base
from sqlalchemy.schema import CreateTable, CreateIndex

print("-- =============================================")
print("-- Jobs Pipeline - Database DDL (auto-generated)")
print("-- =============================================")
print()

# CREATE TABLE statements
for table in Base.metadata.sorted_tables:
    print(CreateTable(table).compile(engine))
    print(";")
    print()

# CREATE INDEX statements
for table in Base.metadata.sorted_tables:
    for index in table.indexes:
        print(CreateIndex(index).compile(engine))
        print(";")
        print()
