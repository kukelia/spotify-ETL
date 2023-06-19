from sqlalchemy import create_engine

engine = create_engine('postgresql://username:password@db:5432/database')