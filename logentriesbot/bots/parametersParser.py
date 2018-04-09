from logentriesbot.helpers import implode


class ParametersParser(object):

    def __init__(self, paramsSpec):
        self.spec = paramsSpec

    def parse(self, params):
        parsed_params = {}

        if params is None:
            error_message = "No params specified.\nCompatible parameters: "
            error_message += "`{0}`".format(
                implode("`, `", self.get_spec_params())
            )

            raise Exception(error_message)

        incompatible_params = self.get_incompatible_params(params)
        if len(incompatible_params) > 0:
            error_message = "Incompatible parameters used: "
            error_message += "`{0}`".format(
                implode("`, `", incompatible_params)
            )

            raise Exception(error_message)

        self.validate_required_params(params)

        for param in params:
            parsed_params[param["name"]] = param["value"]

        return parsed_params

    def get_incompatible_params(self, params):
        not_compatible_params = []

        for param in params:
            if self.is_compatible(param["name"]) is False:
                not_compatible_params.append(param["name"])

        return not_compatible_params

    def is_compatible(self, param_name):
        for param_spec in self.spec:
            if param_spec["name"] == param_name:
                return True

        return False

    def validate_required_params(self, params):
        passed_required_params = self.find_required_params(params)
        required_params = self.get_required_params()

        not_passed_required_params = list(
            set(required_params) - set(passed_required_params)
        )

        if len(not_passed_required_params) > 0:
            error_message = "Missing required params: "
            error_message += implode(", ", not_passed_required_params)

            raise Exception(error_message)

    def find_required_params(self, params):
        passed_required_params = []

        for param in params:
            if self.is_required(param["name"]):
                passed_required_params.append(param["name"])

        return passed_required_params

    def get_required_params(self):
        required_params = []

        for param_spec in self.spec:
            if self.is_required(param_spec["name"]):
                required_params.append(param_spec["name"])

        return required_params

    def get_spec_params(self):
        compatible_params = []

        for params_spec in self.spec:
            compatible_params.append(params_spec["name"])

        return compatible_params

    def is_required(self, param_name):
        is_required = False

        for param_spec in self.spec:
            if param_spec["name"] == param_name and param_spec["required"] is True:
                is_required = True

        return is_required
