
import samweb_client
import http

import os, pwd

class SAMWebClient(object):
    _experiment = os.environ.get('SAM_EXPERIMENT')
    _baseurl = os.environ.get('SAM_WEB_BASE_URL')
    _basesslurl = os.environ.get('SAM_WEB_BASE_SSL_URL')
    _group = os.environ.get('SAM_GROUP')
    _station = os.environ.get('SAM_STATION')

    def __init__(self, experiment=None, secure=False, cert=None, key=None, devel=False):
        if experiment is not None: self.experiment = experiment
        self.secure = secure
        self.devel = devel
        self.set_client_certificate(cert, key)

    def get_experiment(self):
        if self._experiment is None:
            raise samweb_client.Error("Experiment is not defined")
        return self._experiment

    def set_experiment(self, experiment):
        self._experiment = experiment
        if self._experiment.endswith('/dev'):
            self.devel = True
            self._experiment = self._experiment[:-4]

    experiment = property(get_experiment, set_experiment)

    def set_client_certificate(self, cert, key=None):
        http.use_client_certificate(cert, key)

    def get_baseurl(self, secure=None):
        secure = secure or self.secure 
        if not secure and self._baseurl is not None:
            return self._baseurl
        if secure and self._basesslurl is not None:
            return self._basesslurl

        if self.devel:
            path = "/sam/%s/dev/api" % self.experiment
        else:
            path = "/sam/%s/api" % self.experiment
        if secure:
            if self._baseurl:
                import sys
                sys.stderr.write('Warning: BASEURL is set, but using default SSL URL')
            return "https://samweb.fnal.gov:8483%s" % path
        else:
            return "http://samweb.fnal.gov:8480%s" % path

    baseurl = property(get_baseurl)

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

    def get_user(self):
        return pwd.getpwuid(os.getuid()).pw_name

    user = property(get_user)

    def _prepareURL(self, url, secure=None):
        # if provided with a relative url, add the baseurl
        if '://' not in url:
            url = self.get_baseurl(secure) + url
        return url

    def getURL(self, url, args=None, secure=None, *cmdargs, **kwargs):
        url = self._prepareURL(url, secure)
        return http.getURL(url, args, *cmdargs, **kwargs)

    def postURL(self, url, args, secure=None, *cmdargs, **kwargs):
        url = self._prepareURL(url, secure)
        return http.postURL(url, args, *cmdargs, **kwargs)

def samweb_method(m):
    """ Attach this function as a method of the SAMWebClient class """
    setattr(SAMWebClient, m.func_name, m)
    return m
