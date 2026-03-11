from datetime import datetime

from pydantic import BaseModel


class ReplayJob(BaseModel):
    replay_job_id: str
    trading_date: str
    status: str
    created_at_utc: datetime

