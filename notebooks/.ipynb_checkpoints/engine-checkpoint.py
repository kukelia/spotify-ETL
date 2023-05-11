from sqlalchemy import create_engine ,text
from sqlalchemy.pool import StaticPool

engine = create_engine('postgresql://username:password@db:5432/database', poolclass =StaticPool,isolation_level="AUTOCOMMIT")