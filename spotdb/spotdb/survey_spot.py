import json
import os.path
import datetime

from .spotdb_base import SpotDB, SpotDBError


def read_results(json_name):
    with open(json_name, 'r') as f:
        data = json.load(f)
    return data


class SurveyRun:
    def __init__(self, prefix, root=None):
        self.prefix = prefix
        if root:
            self.metadata_file = os.path.join(root, prefix + '-metadata.json')
            self.results_file = os.path.join(root, prefix + '-report.json')
        else:
            self.metadata_file = prefix + '-metadata.json'
            self.results_file = prefix + '-report.json'
        self.ranks = None
        self.threads = None
        self.metadata = None
        self.results = None
        self.locate_run()
        self.get_ranks()
        self.get_threads()
        self.launch_time = datetime.datetime.strptime(self.metadata['executable']['launch_time'], '%Y/%m/%d, %H:%M:%S')
        self.executable_name = os.path.basename(self.metadata['executable']['executable_path'])

    def locate_run(self):
        if not (os.path.exists(self.metadata_file) and os.path.exists(self.results_file)):
            raise Exception('SurveyRun: locate_run',
                            'Could not locate either ' + self.metadata_file + ' or ' + self.results_file)
        self.metadata = read_results(self.metadata_file)
        self.results = read_results(self.results_file)

    def get_ranks(self):
        if 'num_ranks' in self.metadata['executable']:
            self.ranks = self.metadata['executable']['num_ranks']
        else:
            self.ranks = None

    def get_threads(self):
        if 'num_threads' in self.metadata['executable']:
            self.threads = self.metadata['executable']['num_threads']
        else:
            self.threads = None

    def get_run_id(self):
        return '_'.join([self.executable_name, str(self.launch_time.timestamp())])


class SurveySpot(SpotDB):
    def __init__(self, dir_name, meas_type='avg'):
        if not os.path.isdir(dir_name):
            raise SpotDBError('{} is not a valid directory'.format(dir_name))
        self.directory = os.path.abspath(dir_name)
        self.runs = {}
        self.meas_type = meas_type
        self.meas_names_and_types = {}
        self.metadata_names_and_types = {}
        self.metadata = {}
        self.results = {}

    def get_channel_data(self, channel_name, run_ids):
        pass

    def get_regionprofiles(self, run_ids):
        return_dict = {}
        for run_id in run_ids:
            if run_id in self.results:
                return_dict[run_id] = {'runtime': self.results[run_id]}
            else:
                self._parse_run_results(run_id)
                return_dict[run_id] = {'runtime': self.results[run_id]}
        return return_dict

    def get_global_data(self, run_ids):
        """
        Get metadata for runs
        :param run_ids: List of runs
        """
        return_dict = {}
        for run_id in run_ids:
            if run_id in self.metadata:
                return_dict[run_id] = self.metadata[run_id]
            else:
                self._parse_run_metadata(run_id)
                return_dict[run_id] = self.metadata[run_id]
        return return_dict

    def get_metric_attribute_metadata(self):
        for run_id, run in self.runs.items():
            self._parse_run_results(run_id)
        return self.meas_names_and_types

    def get_global_attribute_metadata(self):
        for run_id, run in self.runs.items():
            self._parse_run_metadata(run_id)
        return self.metadata_names_and_types

    def get_all_run_ids(self):
        return self.get_new_runs(None)

    def get_new_runs(self, last_read_time):
        new_runs = []
        for root, dirs, files in os.walk(self.directory, followlinks=True, topdown=True):
            # Survey report files always end in '-report.json'
            report_files = [f for f in files if f.endswith('-report.json')]
            for f in report_files:
                if last_read_time and self._file_is_old(os.path.join(root, f), last_read_time):
                    continue
                parts = f.split('-')
                prefix = '-'.join(parts[:-1])
                try:
                    run = SurveyRun(prefix, root)
                    run_id = run.get_run_id()
                    self.runs[run_id] = run
                    new_runs.append(run_id)
                # If there aren't matching metadata and results files, ignore this prefix
                except:
                    pass
        return new_runs

    def _parse_run_results(self, run_id):
        if run_id not in self.results:
            self.results[run_id] = {}
        for category, measure_collection in self.runs[run_id].results.items():
            # Run data is in the following format. Measurements are either stored in min/max/avg or value:
            # run.results = { 'category 1': { 'meas 1': { 'min': value, 'max': value, 'avg': value },
            #                               { 'meas 2: { 'value': value }},
            #               { 'category 2': etc
            #
            # Ignore these categories for now. Affinity is not a measurement. app_data has a user-specified
            # structure. gpu_data is also from an external program.
            if category in ['AFFINITY', 'app_data', 'gpu_data']:
                continue
            for name, measure in measure_collection.items():
                if self.meas_type in measure:
                    self.results[run_id][name] = measure[self.meas_type]
                    self._update_field_names_and_types(self.meas_names_and_types, name.strip(),
                                                       self._get_type(measure[self.meas_type]))
                elif 'value' in measure:
                    self.results[run_id][name] = measure['value']
                    self._update_field_names_and_types(self.meas_names_and_types, name.strip(),
                                                       self._get_type(measure['value']))

    def _parse_run_metadata(self, run_id):
        if run_id not in self.metadata:
            self.metadata[run_id] = {}
        for category, cat_items in self.runs[run_id].metadata.items():
            # Metadata is stored in the format:
            # run.metadata = { 'executable': { 'name': value }}
            # However, value may be a simple string, a boolean, or another dictionary.
            # Some items have nested dictionaries, so we've special-cased those items below:
            for name, item in cat_items.items():
                # Exclude some survey-specific metadata values
                if name in ['survey_arguments', 'config_file'] or item is None or type(item) == bool:
                    continue
                # cpu_info is a dictionary. Grabbing these sub-items for spot
                if name == 'cpu_info':
                    for cpu_name, value in item.items():
                        if type(value) == bool:
                            continue
                        self.metadata[run_id][cpu_name] = value
                        self._update_field_names_and_types(self.metadata_names_and_types, cpu_name, 'string')
                # lsmem is a dictionary. Some items are string values, but there is another embedded dictionary
                # of values as well. Turning that dictionary into a json block
                elif name == 'mem':
                    for mem_name, value in item.items():
                        if mem_name == 'BLOCKS':
                            self.metadata[run_id]['mem_blocks'] = json.dumps(item['BLOCKS'])
                            self._update_field_names_and_types(self.metadata_names_and_types, 'mem_blocks', 'string')
                        else:
                            self.metadata[run_id][mem_name] = value
                            self._update_field_names_and_types(self.metadata_names_and_types, mem_name, 'string')
                # Matching user_name to the caliper field 'name'
                elif name == 'user_name':
                    self.metadata[run_id]['name'] = item
                    self._update_field_names_and_types(self.metadata_names_and_types, 'name', 'string')
                # Changing launch_time into spot-expected launchdate as an epoch value.
                # Also keeping old value of launch_time to easily match to the corresponding end_time
                elif name == 'launch_time':
                    self.metadata[run_id]['launchdate'] = int(round(self.runs[run_id].launch_time.timestamp()))
                    self.metadata[run_id]['launch_time'] = item
                    self._update_field_names_and_types(self.metadata_names_and_types, 'launchdate', 'date')
                    self._update_field_names_and_types(self.metadata_names_and_types, 'launch_time', 'string')
                # Change other dictionaries and lists into json strings
                elif type(item) == dict or type(item) == list:
                    self.metadata[run_id][name] = json.dumps(item)
                    self._update_field_names_and_types(self.metadata_names_and_types, name, 'string')
                elif type(item) == str:
                    self.metadata[run_id][name] = item
                    self._update_field_names_and_types(self.metadata_names_and_types, name, 'string')
                # Remaining items must be numerical
                else:
                    self.metadata[run_id][name] = item
                    self._update_field_names_and_types(self.metadata_names_and_types, name, self._get_type(item))

    @staticmethod
    def _update_field_names_and_types(field, name, type_string):
        # I'm not sure if it's really worth checking for existence before just writing it
        if name not in field:
            field[name] = {'type': type_string}

    @staticmethod
    def _file_is_old(file_name, last_read_time):
        if os.stat(file_name).st_ctime > last_read_time:
            return False
        return True

    @staticmethod
    def _get_type(value):
        if type(value) == float:
            ret = 'float'
        elif type(value) == int:
            ret = 'int'
        else:
            ret = 'string'
        return ret

