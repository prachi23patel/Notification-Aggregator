import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base


load_dotenv()

db_url = os.getenv("DATABASE_URL")

engine = create_async_engine(
    db_url, 
    echo=False,
)

# Every time a request comes in: A new session is opened , Session does the database work (read/write), Session is closed after work is done
# Why close it after every request?To avoid memory leaks , To avoid too many open connections , To keep the app clean and fast
sessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False # Prevents errors when accessing attributes after a commit in async
)#Creates a “session blueprint”

Base = declarative_base()
# declarative_base() creates a parent class that allows SQLAlchemy to recognize and manage your Python classes as database tables.
async def get_db():
    # Creates a real async session connected to the DB connection pool
    async with sessionLocal() as db:
        yield db
    # The 'async with' context manager guarantees the session is safely closed 
    # and returned to the pool after the router finishes, avoiding any memory leaks.

# engine = create_engine(db_url, echo=False)
# What it means: 
# It creates an SQLAlchemy Engine. This is the core gatekeeper between your Python code and PostgreSQL.Does it connect immediately? No. It just prepares the configuration. The actual connection to the database isn't opened until you try to execute your first query or open a session.
# The Connection Pool:
#  By default, the Engine automatically sets up a connection pool (usually starting with 5 persistent connections). When a request comes in, it grabs an already-open connection from this pool, uses it, and puts it right back when done.


# sessionLocal = sessionmaker(bind=engine)
# What it means:
#  This creates a Session Factory (or blueprint). It doesn't create a database session yet. It just builds a custom class that knows exactly how to spin up a new session linked to your engine whenever you call it.
# In your FastAPI routes:
#  When you use db = sessionLocal(), you are using this factory to get a distinct database workspace (a transaction block) for that specific API request.


