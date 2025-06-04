from sqlalchemy import Column, Integer, BigInteger, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(BigInteger, nullable=False)
    channel = Column(String(255), nullable=False)
    post_date = Column(DateTime, nullable=False)
    post_text = Column(Text)
    published = Column(Boolean, default=False)
    is_album = Column(Boolean, default=False)  # Флаг альбома
    created_at = Column(DateTime, default=datetime.utcnow)

    media = relationship(
        "Media",
        back_populates="post",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

class Media(Base):
    __tablename__ = 'media'
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete='CASCADE'))
    file_path = Column(String(500), nullable=False)
    media_type = Column(String(50), nullable=False)

    post = relationship("Post", back_populates="media")