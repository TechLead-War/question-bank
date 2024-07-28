import os


class Config:
    POSTGRES = {
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'pw': os.getenv('POSTGRES_PASSWORD', 'password'),
        'db': os.getenv('POSTGRES_DB', 'mcq'),
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '6432'),
        'schema': 'mcq'
    }
