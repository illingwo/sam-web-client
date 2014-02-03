
import samweb_client
import http_client

import os, pwd

class ExperimentNotDefined(samweb_client.Error): pass

_version = None
def get_version():
    """ Get the version somehow """
    global _version
    if _version is None:

        # first try the baked in value
        try:
            from _version import version
            _version = version
        except ImportError:
            _version = 'unknown'

        # if running from a git checkout, try to obtain the version
        gitdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.git")
        if os.path.exists(gitdir):
            import subprocess
            try:
                p = subprocess.Popen(["git", "--work-tree=%s" % os.path.join(gitdir,".."), "--git-dir=%s" % gitdir, "describe", "--tags", "--dirty"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if p.wait() == 0:
                    _version = p.stdout.read().strip()
            except: pass
    return _version

class SAMWebClient(object):
    _experiment = os.environ.get('SAM_EXPERIMENT')
    _baseurl = os.environ.get('SAM_WEB_BASE_URL')
    _basesslurl = os.environ.get('SAM_WEB_BASE_SSL_URL')
    _group = os.environ.get('SAM_GROUP')
    _station = os.environ.get('SAM_STATION')

    # SAM role to use
    _default_role = 'default'

    def __init__(self, experiment=None, secure=False, cert=None, key=None, devel=None):
        self.devel = False
        if experiment is not None: self.experiment = experiment
        self.secure = secure
        if devel is not None: self.devel = devel
        self.http_client = http_client.get_client()
        self.set_client_certificate(cert, key)
        self.role = None

    def get_role(self):
        return self._role
    def set_role(self, newval):
        if newval is None:
            self._role = self._default_role
        else:
            self._role = newval

    role = property(get_role, set_role)

    def get_experiment(self):
        if self._experiment is None:
            raise ExperimentNotDefined("Experiment is not defined")
        return self._experiment

    def set_experiment(self, experiment):
        self._experiment = experiment
        if self._experiment.endswith('/dev'):
            self.devel = True
            self._experiment = self._experiment[:-4]

    experiment = property(get_experiment, set_experiment)

    def set_client_certificate(self, cert, key=None):
        self.http_client.use_client_certificate(cert, key)

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

    def getURL(self, url, params=None, secure=None, role=None,  *args, **kwargs):
        url = self._prepareURL(url, secure)
        if secure: kwargs['role'] = role or self.role
        return self.http_client.getURL(url, params=params, *args, **kwargs)

    def postURL(self, url, data=None, secure=None, role=None, *args, **kwargs):
        url = self._prepareURL(url, secure)
        if secure: kwargs['role'] = role or self.role
        return self.http_client.postURL(url, data=data, *args, **kwargs)

    def putURL(self, url, data=None, secure=None, role=None, *args, **kwargs):
        url = self._prepareURL(url, secure)
        if secure: kwargs['role'] = role or self.role
        return self.http_client.putURL(url, data=data, *args, **kwargs)

    def deleteURL(self, url, params=None, secure=None, role=None, *args, **kwargs):
        url = self._prepareURL(url, secure)
        if secure: kwargs['role'] = role or self.role
        return self.http_client.deleteURL(url, params=params, *args, **kwargs)

    def get_verbose(self):
        return self.http_client.verbose
    def set_verbose(self, verbose):
        self.http_client.verbose = verbose

    verbose = property(get_verbose, set_verbose)

def samweb_method(m):
    """ Attach this function as a method of the SAMWebClient class """
    setattr(SAMWebClient, m.func_name, m)
    return m
