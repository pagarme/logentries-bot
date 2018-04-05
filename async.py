import asyncio
from datetime import datetime, timedelta
from prettyconf import config
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from logentriesbot.client.logentries import LogentriesConnection, Query
from logentriesbot.client.logentrieshelper import LogentriesHelper


job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

scheduler = AsyncIOScheduler(job_defaults=job_defaults)


async def do_request(url):
    client = LogentriesConnection(config('LOGENTRIES_API_KEY'))
    response = await client.get_async(url)
    print(len(response['events']))


async def kill_task(id):
    scheduler.remove_job(id)


async def request():
    logs = await LogentriesHelper.get_all_live_environment_async()

    query = Query()\
        .where('method="POST"')\
        .and_('/transactions/')\
        .logs(logs)

    client = LogentriesConnection(config('LOGENTRIES_API_KEY'))

    response = await client.post_async('/query/live/logs', query.build())
    continue_url = response['links'][0]['href'][27:]

    time = datetime.now() + timedelta(seconds=20)
    print(time)

    scheduler.add_job(
        do_request,
        'interval',
        [continue_url],
        seconds=1,
        id='requests'
    )

    scheduler.add_job(
        kill_task,
        'date',
        run_date=time,
        args=['requests']
    )


async def main():
    scheduler.start()

    await request()


ioloop = asyncio.get_event_loop()

ioloop.create_task(main())

ioloop.run_forever()
ioloop.close()
