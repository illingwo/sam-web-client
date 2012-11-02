
import os, pwd

class Error(Exception):
  pass

class _SAMWebConnectInfo(object):
    _experiment = os.environ.get('SAM_EXPERIMENT')
    _baseurl = os.environ.get('SAM_WEB_BASE_URL')
    _basesslurl = os.environ.get('SAM_WEB_BASE_SSL_URL')
    _group = os.environ.get('SAM_GROUP')
    _station = os.environ.get('SAM_STATION')

    def __init__(self):
        self.secure = False
        self.devel = False

    def get_experiment(self):
        if self._experiment is None:
            raise Error("Experiment is not defined")
        return self._experiment

    def set_experiment(self, experiment):
        self._experiment = experiment
        if self._experiment.endswith('/dev'):
            self.devel = True
            self._experiment = self._experiment[:-4]

    experiment = property(get_experiment, set_experiment)

    @property
    def baseurl(self):
        if not self.secure and self._baseurl is not None:
            return self._baseurl
        if self.secure and self._basesslurl is not None:
            return self._basesslurl

        if self.devel:
            path = "/sam/%s/dev/api" % self.experiment
        else:
            path = "/sam/%s/api" % self.experiment
        if self.secure:
            return "https://samweb.fnal.gov:8483%s" % path
        else:
            return "http://samweb.fnal.gov:8480%s" % path

    def get_group(self):
        return self._group or self.get_experiment()

    def set_group(self,group):
        self._group = group

    group = property(get_group, set_group)

    def get_station(self):
        return self._station or self.get_experiment()

    def set_station(self,station):
        self._station = station

    station = property(get_station, set_station)

    def get_user():
        return pwd.getpwuid(os.getuid()).pw_name

    user = property(get_user)


samweb_connect = _SAMWebConnectInfo()

