from sqlalchemy import String, Text, DateTime, func,Index, ForeignKeyConstraint, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List
from datetime import datetime, timezone

class Base(DeclarativeBase):
    pass

class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int]             = mapped_column(primary_key=True)
    post_id: Mapped[int]        = mapped_column(index=False)
    channel_name: Mapped[str]   = mapped_column(String(100), index=False)
    text: Mapped[Optional[str]] = mapped_column(Text())
    published: Mapped[bool]     = mapped_column(Boolean, default=False, index=True)
    date: Mapped[datetime]      = mapped_column(DateTime(timezone=True), index=True)
    scraped_at: Mapped[datetime]= mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("ix_posts_channel_post", "channel_name", "post_id", unique=True),
    )

    media: Mapped[List["Media"]] = relationship(
        "Media",
        back_populates="post",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"Post(id={self.id}, channel={self.channel_name}, post_id={self.post_id}, date={self.date})"


class Media(Base):
    __tablename__ = "media"

    id: Mapped[int]             = mapped_column(primary_key=True)
    post_id: Mapped[int]        = mapped_column(index=False)
    channel_name: Mapped[str]   = mapped_column(String(100), index=False)
    media_type: Mapped[str]     = mapped_column(String(50))
    file_path: Mapped[str] = mapped_column(String(511))

    __table_args__ = (
        ForeignKeyConstraint(
            ["post_id", "channel_name"],
            ["posts.post_id", "posts.channel_name"],
            ondelete="CASCADE",
            name="fk_media_post"
        ),
        Index("ix_media_post_channel", "post_id", "channel_name"),
    )

    post: Mapped["Post"] = relationship(
        "Post",
        back_populates="media"
    )

    def __repr__(self):
        return f"Media(id={self.id}, type={self.media_type}, channel_name={self.channel_name} post_id={self.post_id})"
