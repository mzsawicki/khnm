import datetime


class TestClock:
    def __init__(self, start_time: datetime.datetime) -> None:
        self.time = start_time

    def now(self) -> datetime.datetime:
        return self.time

    async def sleep(self, time_seconds: int) -> None:
        self.time += datetime.timedelta(seconds=time_seconds)
