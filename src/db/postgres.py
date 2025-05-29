import os
from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
from src.db.model import Base

class PostgresDB:
    def __init__(self):
        """Inisialisasi koneksi database menggunakan variabel lingkungan"""
        load_dotenv()
        
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        
        self.engine = None
        self.session = None
        self.SessionLocal = None
        
    def connect(self):
        """Membuat koneksi ke database menggunakan SQLAlchemy"""
        try:
            connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.host}:{self.port}/{self.db_name}"
            self.engine = create_engine(connection_string)
            
            # Buat session factory
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            # Buat session untuk digunakan
            self.session = self.SessionLocal()
            
            # Coba koneksi
            self.engine.connect()
            print("Database connection established successfully")
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Menutup koneksi database"""
        if self.session:
            self.session.close()
            print("Database session closed")
    
    def create_tables(self):
        """Membuat semua tabel yang didefinisikan"""
        Base.metadata.create_all(bind=self.engine)
        print("All tables created")
    
    def create_record(self, model_class, data):
        try:
            # Buat instance model dengan data
            record = model_class(**data)
            
            # Tambahkan ke session
            self.session.add(record)
            
            # Commit perubahan
            self.session.commit()
            
            # Refresh untuk mendapatkan ID yang dibuat
            self.session.refresh(record)
            
            print(f"Record created successfully with ID: {record.id}")
            return record.id
        except Exception as e:
            self.session.rollback()
            print(f"Error creating record: {e}")
            return False
    
    def read_records(self, model_class, filters=None, limit=None):
        try:
            # Buat query dasar
            query = self.session.query(model_class)
            
            # Tambahkan filter jika ada
            if filters:
                for column, value in filters.items():
                    query = query.filter(getattr(model_class, column) == value)
            
            # Tambahkan limit jika ada
            if limit:
                query = query.limit(limit)
            
            # Jalankan query
            records = query.all()
            
            return records
        except Exception as e:
            print(f"Error reading records: {e}")
            return []