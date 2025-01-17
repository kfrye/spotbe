from abc import ABC, abstractmethod
import re

class SpotDBError(Exception):
    """ SpotDB Error base class """

    def __init__(self, msg):
        self.msg = msg


class SpotDB(ABC):
    """ SpotDB base class """

    @abstractmethod
    def get_global_attribute_metadata(self):
        """ Return a dict with metadata (e.g., data types) for all global
            attributes found in the database.
        """
        pass

    @abstractmethod
    def get_metric_attribute_metadata(self):
        """ Return a dict with metadata (e.g., aliases) for all metric
            attributes found in the database.
        """
        pass

    @abstractmethod
    def get_all_run_ids(self):
        """ Return a list with all run ids in the database
        """
        pass

    @abstractmethod
    def get_new_runs(self, last_read_time):
        """ Return list of run ids newer than last_read """
        pass

    @abstractmethod
    def get_global_data(self, run_ids):
        """ Return global attributes for the given list of run_ids

            Structure:

            {
                run_id_1: { "attribute1": value1, "attribute2": value2, ... },
                run_id_2: { "attribute1": value1, "attribute2": value2, ... },
                ...
            }
        """
        pass

    @abstractmethod
    def get_regionprofiles(self, run_ids):
        """ Return region profile data for the given list of run ids

            Structure:

            {
                run_id_1:
                    {
                        "main":     { "metric": val, ... },
                        "main/foo": { "metric": val, ... },
                        ...
                    },
                run_id_2: { ... }, ...
            }
        """
        pass

    @abstractmethod
    def get_channel_data(self, channel_name, run_ids):
        """ Return channel data (e.g., timeseries profile) for the
            given list of run ids and channel

            Structure:

            {
                run_id_1: [
                    { "attribute": "value", ... },
                    { "attribute": "value", ... },
                    ...
                ],
                run_id_2: [
                    ...
                ],
                ...
            }
        """
        pass

    # def query(self, query):
    #     """ Return list of run ids matching the given query """
    #     pass


def connect(database_key,read_only=False):
    """ Return a SpotDB object for the given database key
        (directory or SQL connection string)
    """

    import os

    if os.path.isdir(database_key):
        if 'survey' in database_key:
            from .survey_spot import SurveySpot
            return SurveySpot(database_key)
        from .calidirdb import SpotCaliperDirectoryDB
        return SpotCaliperDirectoryDB(database_key)
    elif database_key.endswith(".sqlite") or database_key.startswith("mysql"):
        from .sinadb import SpotSinaDB
        return SpotSinaDB(database_key, read_only=read_only)
    elif database_key.endswith(".cnf"):
        from .sqlutil import make_sql_uri_from_cnf
        from .sinadb import SpotSinaDB
        uri = make_sql_uri_from_cnf(database_key)
        return SpotSinaDB(uri, read_only=read_only)
    else:
        raise SpotDBError("Unknown Spot database format: " + database_key)
