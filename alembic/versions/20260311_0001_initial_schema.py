"""initial schema"""

from alembic import op

from libs.db.base import Base
from libs.db import models  # noqa: F401

revision = "20260311_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
