from logentriesbot.client.logentries import LogentriesConnection
from datetime import datetime, timedelta
from prettyconf import config


class LogentriesHelper(object):

    @staticmethod
    def get_all_live_environment():
        all_live_environment_id = 'acd35399-1eb3-45f2-a79e-701b9733a50c'

        return LogentriesConnection(
            config('LOGENTRIES_API_KEY')
        ).get_logset_logs(all_live_environment_id)

    @staticmethod
    async def get_all_live_environment_async():
        all_live_environment_id = 'acd35399-1eb3-45f2-a79e-701b9733a50c'

        return await LogentriesConnection(
            config('LOGENTRIES_API_KEY')
        ).get_logset_logs_async(all_live_environment_id)

    @staticmethod
    def get_all_test_environment():
        all_test_environment_id = 'a908909c-9217-4feb-83b7-0ed4798fca3a'

        return LogentriesConnection(
            config('LOGENTRIES_API_KEY')
        ).get_logset_logs(all_test_environment_id)

    @staticmethod
    async def get_all_test_environment_async():
        all_test_environment_id = 'a908909c-9217-4feb-83b7-0ed4798fca3a'

        return await LogentriesConnection(
            config('LOGENTRIES_API_KEY')
        ).get_logset_logs_async(all_test_environment_id)


class Time(object):

    @staticmethod
    def parse(quantity, unit):
        kwargs = {unit: quantity}

        return timedelta(**kwargs)

    @staticmethod
    def get_timestamp(date_string):
        dt = datetime.strptime(date_string, '%d/%m/%Y %H:%M:%S')
        timestamp = int(dt.timestamp() * 1000)
        return timestamp

    @staticmethod
    def get_interval_as_timestamp(from_dt, to_dt):
        interval = from_dt - to_dt
        timestamp = Time.get_timestamp(
            interval.strftime('%d/%m/%Y %H:%M:%S')
        )

        return timestamp
