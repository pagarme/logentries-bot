from logentriesbot.bots.bot import Bot
from logentriesbot.bots.parametersParser import ParametersParser
from logentriesbot.client.logentries import LogentriesConnection, Query
from logentriesbot.client.logentrieshelper import LogentriesHelper, Time
from logentriesbot.monitoring import add_company, remove_company, get_jobs, deploy
from prettyconf import config


class LogWatcher(Bot):

    def __init__(self, bot_name, slack_connection):
        Bot.__init__(self, bot_name, slack_connection)

        self.commands = {
            "add": {
                "fn": self.add,
                "params": [
                    {"name": "company_id", "required": True},
                    {"name": "status_code", "required": True},
                    {"name": "error_message", "required": True},
                    {"name": "quantity", "required": True},
                    {"name": "unit", "required": True}
                ],
                "async": True
            },
            "remove": {
                "fn": self.remove,
                "params": [
                    {"name": "job_id", "required": True}
                ],
                "async": True
            },
            "get_jobs": {
                "fn": self.get_jobs,
                "async": True
            },
            "deploy": {
                "fn": self.deploy,
                "params": [
                    {"name": "quantity", "required": True},
                    {"name": "unit", "required": True}
                ],
                "async": True
            },
            "query": {
                "fn": self.query
            },
            "help": {
                "fn": self.help
            }
        }

    def add(self, params, callback):
        params_spec = self.commands["add"]["params"]

        try:
            parsed_params = ParametersParser(params_spec).parse(params)
        except Exception as e:
            callback(str(e))
            return e

        company_id = parsed_params["company_id"]
        quantity = int(parsed_params["quantity"])
        unit = parsed_params["unit"]
        status_code = parsed_params["status_code"]
        error_message = parsed_params["error_message"]

        return add_company(
            company_id, quantity, unit,
            callback, status_code, error_message
        )

    def remove(self, params, callback):
        params_spec = self.commands["remove"]["params"]

        try:
            parsed_params = ParametersParser(params_spec).parse(params)
        except Exception as e:
            callback(str(e))
            return e

        job_id = parsed_params["job_id"]

        return remove_company(job_id, callback)

    def get_jobs(self, params, callback):
        return get_jobs(callback)

    def deploy(self, params, callback):
        params_spec = self.commands["deploy"]["params"]

        try:
            parsed_params = ParametersParser(params_spec).parse(params)
        except Exception as e:
            callback(str(e))
            return e

        quantity = int(parsed_params["quantity"])
        unit = parsed_params["unit"]

        return deploy(quantity, unit, callback)

    def query(self, params):
        for c in params:
            if c['name'] == 'query':
                statement = c['value']
            elif c['name'] == 'from':
                from_time = Time.get_timestamp(c['value'])
            elif c['name'] == 'to':
                to_time = Time.get_timestamp(c['value'])

        logs = LogentriesHelper.get_all_test_environment()
        + LogentriesHelper.get_all_live_environment()

        query = Query(statement, {
            'from': from_time, 'to': to_time
        }, logs)

        client = LogentriesConnection(config('LOGENTRIES_API_KEY'))
        return client.post('/query/logs', query.build())

    def help(self, params=None):
        response = "Currently I support the following commands:\r\n"

        for command in self.commands:
            response += command + "\r\n"

        return response
