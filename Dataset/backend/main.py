from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker, Session, declarative_base
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datasets import Dataset
import io

# Database setup
DATABASE_URL = "sqlite:///./articles.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# SQLAlchemy model
class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, index=True)
    news_article = Column(String)
    summary = Column(String)
    bias_religious = Column(Boolean, default=False)
    bias_cultural = Column(Boolean, default=False)
    bias_language = Column(Boolean, default=False)
    bias_gender = Column(Boolean, default=False)
    bias_pro_gov = Column(Boolean, default=False)
    bias_anti_gov = Column(Boolean, default=False)

Base.metadata.create_all(bind=engine)

# Pydantic model for request body
class ArticleCreate(BaseModel):
    url: str
    news_article: str
    summary: str
    bias_religious: bool = False
    bias_cultural: bool = False
    bias_language: bool = False
    bias_gender: bool = False
    bias_pro_gov: bool = False
    bias_anti_gov: bool = False

# Pydantic model for response
class ArticleResponse(ArticleCreate):
    id: int

    class Config:
        from_attributes = True

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/articles/", response_model=ArticleResponse)
def create_article(article: ArticleCreate, db: Session = Depends(get_db)):
    db_article = Article(**article.model_dump())
    db.add(db_article)
    db.commit()
    db.refresh(db_article)
    return db_article

@app.get("/articles/", response_model=list[ArticleResponse])
def read_articles(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    articles = db.query(Article).offset(skip).limit(limit).all()
    return articles

def get_articles_as_df(db: Session):
    articles = db.query(Article).all()
    if not articles:
        return pd.DataFrame()
    # Manually remove the SQLAlchemy instance state
    article_list = []
    for article in articles:
        d = article.__dict__.copy()
        d.pop('_sa_instance_state', None)
        article_list.append(d)
    return pd.DataFrame(article_list)

@app.get("/articles/csv")
def export_articles_csv(db: Session = Depends(get_db)):
    df = get_articles_as_df(db)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=articles.csv"
    return response

@app.get("/articles/parquet")
def export_articles_parquet(db: Session = Depends(get_db)):
    df = get_articles_as_df(db)
    stream = io.BytesIO()
    df.to_parquet(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="application/octet-stream")
    response.headers["Content-Disposition"] = "attachment; filename=articles.parquet"
    return response

@app.get("/articles/dataset")
def export_articles_dataset(db: Session = Depends(get_db)):
    df = get_articles_as_df(db)
    if df.empty:
        return {"data": []}
    dataset = Dataset.from_pandas(df)
    return dataset.to_dict()
