import ast
import json
import uuid
from urllib.parse import quote
from datetime import datetime
from prettyconf import config
from apscheduler.schedulers.background import BackgroundScheduler

from logentriesbot.client.logentries import LogentriesConnection, Query
from logentriesbot.client.logentrieshelper import LogentriesHelper, Time
from logentriesbot.helpers import implode
from logentriesbot.client.slack import SlackAttachment

scheduler = BackgroundScheduler()
scheduler.start()


def check(job_id, company_id, quantity, unit, callback, status_code=400):
    parsed_query_interval = Time.parse(quantity, unit)
    from_time = Time.get_interval_as_timestamp(
        datetime.now(), parsed_query_interval
    )

    errors = get_how_many(company_id, from_time, status_code)

    link = "https://logentries.com/app/73cd17bb#/search/logs/?log_q={}".format(quote(errors["query"]))

    report = "{} errors in last {} {}".format(
        errors['errors'], str(quantity), unit
    )

    attachment = SlackAttachment("#EA1212")\
        .field(title="Company", value=company_id, short=True)\
        .field(title="Status", value=report, short=True)\
        .field(title="Job ID", value=job_id, short=True)\
        .action(name="Run It", text="Run It!", type="button", url=link)

    callback(json.dumps([
        attachment.build()
    ]))


def check_messages(job_id, company_id, quantity, unit, callback, status_code=400):
    parsed_query_interval = Time.parse(quantity, unit)
    from_time = Time.get_interval_as_timestamp(
        datetime.now(), parsed_query_interval
    )

    errors = get_how_many_each_error(company_id, from_time, status_code)

    link = "https://logentries.com/app/73cd17bb#/search/logs/?log_q={}".format(
        quote(errors["query"])
    )

    report = "{} failed requests in the last {} {}".format(
        errors['errors']['count'],
        str(quantity),
        unit
    )

    attachment = SlackAttachment("#EA1212")\
        .field(title="Company", value=company_id, short=True)\
        .field(title="Status", value=report, short=True)\
        .action(name="Run It", text="Run It!", type="button", url=link)

    if errors['errors']['count'] > 0:
        attachment.field(
            title="Error Messages",
            value=implode(', ', errors['errors']['messages']),
            short=False
        )

    attachment.field(title="Job Id", value=job_id, short=True)

    callback(json.dumps([
        attachment.build()
    ]))


def add_company(company_id, quantity, unit, callback, status_code=400, error_message=False):
    global scheduler

    # unit must be: minutes, hours, days or weeks
    kwargs = {unit: quantity}

    job_id = str(uuid.uuid4())[:8]

    error_message = True if error_message.lower() == "true" else False

    alert_function = check_messages if error_message is True else check

    scheduler.add_job(
        alert_function,
        'interval',
        [job_id, company_id, quantity, unit, callback, status_code],
        id=job_id,
        **kwargs,
        name=company_id
    )

    attachment = SlackAttachment("#0059EA")\
        .field(title="Company", value=company_id, short=True)\
        .field(title="Job ID", value=job_id, short=True)

    callback(json.dumps([
        attachment.build()
    ]))


def remove_company(job_id, callback):
    global scheduler

    job = scheduler.get_job(job_id=job_id)
    if job:
        company_id = str(job.name)

    try:
        scheduler.pause_job(job_id=job_id)
        scheduler.remove_job(job_id=job_id)
    except Exception:
        callback("Error! Check job_id and try again!")

    attachment = SlackAttachment("#0BE039")\
        .field(title="Job ID", value=job_id, short=True)\
        .field(title="Company", value=company_id, short=True)

    callback(json.dumps([
        attachment.build()
    ]))


def get_how_many(company_id, from_time, status_code=400):

    to_time = Time.get_timestamp(
        datetime.strftime(datetime.now(), "%d/%m/%Y %H:%M:%S")
    )

    logs = (
        LogentriesHelper.get_all_test_environment() +
        LogentriesHelper.get_all_live_environment()
    )

    query = Query()\
        .where("statusCode={}".format(status_code))\
        .and_("id={}".format(company_id))\
        .and_("/POST/")\
        .interval(from_time, to_time)\
        .groupby("_id")\
        .calculate("count")\
        .logs(logs)

    response = LogentriesConnection(
        config('LOGENTRIES_API_KEY')
    ).query(query.build())

    errors = 0
    if len(response['statistics']['groups']) > 0:
        errors = response['statistics']['groups'][0][company_id]['count']

    result = {
        "errors": errors,
        "query": query.to_string()
    }

    return result


def get_how_many_each_error(company_id, from_time, status_code=400):
    to_time = Time.get_timestamp(
        datetime.strftime(datetime.now(), "%d/%m/%Y %H:%M:%S")
    )

    logs = (
        LogentriesHelper.get_all_test_environment() +
        LogentriesHelper.get_all_live_environment()
    )

    query = Query()\
        .where('statusCode={}'.format(status_code))\
        .and_('_id={}'.format(company_id))\
        .and_('/POST/')\
        .interval(from_time, to_time)\
        .logs(logs)

    client = LogentriesConnection(config('LOGENTRIES_API_KEY'))
    response = client.query(query.build())

    latest_errors_summary = {
        'count': 0,
        'messages': []
    }

    for failed_response in response['events']:
        latest_errors_summary['count'] += 1

        response_body = failed_response['message'][1:]
        response_body = ast.literal_eval(response_body)

        for error in response_body['body']['errors']:
            latest_errors_summary['messages'].append(error['message'])

    latest_errors_summary['messages'] = list(
        set(latest_errors_summary['messages'])
    )

    return {
        'query': query.to_string(),
        'errors': latest_errors_summary
    }


def get_jobs(callback):
    global scheduler

    jobs = scheduler.get_jobs()

    callback("Running jobs: ")
    for job in jobs:
        callback("job_id: *{}* watching company *{}*".format(job.id, job.name))
