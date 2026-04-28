"""
Jobs Pipeline - Configuration
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Database
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "13201")
    DB_NAME = os.getenv("DB_NAME", "defaultdb")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            f"?sslmode={self.DB_SSLMODE}"
        )

    # Frontend / Site URLs
    FRONTEND_URL = os.getenv("FRONTEND_URL", "https://careers.annex-technologies.com")
    SITE_URL = os.getenv("SITE_URL", "https://careers.annex-technologies.com")
    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

    # Email (SMTP fallback + HTTP email APIs)
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER", "annexcareerske@gmail.com")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Annex Careers")
    RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
    EMAIL_FROM = os.getenv("EMAIL_FROM", "noreply@annex-technologies.com")
    BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")

    # Scraper
    USER_AGENT = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    )
    SCRAPE_INTERVAL_HOURS = int(os.getenv("SCRAPE_INTERVAL_HOURS", "24"))


settings = Settings()
