"""
Test database connection and create tables.
Run: python test_connection.py
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from database.connection import engine, init_db
from database.models import Job, ScrapeLog
from sqlalchemy import text


def test_connection():
    print("=" * 60)
    print("Jobs Pipeline - Database Connection Test")
    print("=" * 60)
    print(f"Host: {settings.DB_HOST}")
    print(f"Port: {settings.DB_PORT}")
    print(f"Database: {settings.DB_NAME}")
    print(f"User: {settings.DB_USER}")
    print(f"SSL Mode: {settings.DB_SSLMODE}")
    print("-" * 60)

    try:
        # Test basic connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"Connected! PostgreSQL version:")
            print(f"  {version}")
            print()

        # Create tables
        print("Creating tables...")
        init_db()
        print("  - jobs table: OK")
        print("  - scrape_logs table: OK")
        print()

        # Verify tables exist
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' ORDER BY table_name"
                )
            )
            tables = [row[0] for row in result]
            print(f"Tables in database: {tables}")

        print()
        print("All good! Database is ready.")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"Connection FAILED: {e}")
        print("=" * 60)
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
