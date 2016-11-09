
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
    _host = os.environ.get('SAM_WEB_HOST') or 'samweb.fnal.gov'
    _port = os.environ.get('SAM_WEB_PORT') or '8480'
    _sslport = os.environ.get('SAM_WEB_SSL_PORT') or '8483'
    _baseurl = os.environ.get('SAM_WEB_BASE_URL')
    _basesslurl = os.environ.get('SAM_WEB_BASE_SSL_URL')
    if _basesslurl is None and _baseurl and _baseurl.startswith('https:'): _basesslurl = _baseurl
    _group = os.environ.get('SAM_GROUP')
    _station = os.environ.get('SAM_STATION')
    _timezone = os.environ.get('SAM_TZ')

    # SAM role to use
    _default_role = 'default'

    def __init__(self, experiment=None, secure=False, cert=None, key=None, devel=None, timezone=None):
        self.devel = False
        if experiment is not None: self.experiment = experiment
        self.secure = secure
        if devel is not None: self.devel = devel
        self.http_client = http_client.get_client()
        self.set_client_certificate(cert, key)
        self.role = None
        timezone = timezone or self._timezone
        if timezone: self.timezone = timezone

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

    def set_host(host):
        self._host = host

    def set_ports(port, sslport):
        if port: self._port = port
        if sslport: self._sslport = sslport

    def get_baseurl(self, secure=None):
        """ Return the base url. If secure is set this will be an
        https url, if not it may not be """
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
            return "https://%s:%s%s" % (self._host, self._sslport, path)
        else:
            return "http://%s:%s%s" % (self._host, self._port, path)

    baseurl = property(get_baseurl)

    def get_group(self):
        if not self._group:
            # if the group isn't set then get it from the experiment name
            # knocking off anything after a trailing slash (ie hypot/dev -> hypot)
            self._group = self.get_experiment().split('/', 1)[0]
        return self._group

    def set_group(self,group):
        self._group = group

    group = property(get_group, set_group)

    def get_station(self):
        return self._station or self.get_experiment()

    def set_station(self,station):
        self._station = station

    station = property(get_station, set_station)

    def get_user(self):
        return http_client.get_username()

    user = property(get_user)

    def get_timezone(self):
        return self.http_client.timezone

    def set_timezone(self, new_tz):
        self.http_client.timezone = new_tz

    timezone = property(get_timezone, set_timezone)

    def _prepareURL(self, url, secure=None):
        # if provided with a relative url, add the baseurl
        if '://' not in url:
            url = self.get_baseurl(secure) + url
        return url

    def getURL(self, url, params=None, *args, **kwargs):
        return self._doURL(self.http_client.getURL, url, params=params, *args, **kwargs)

    def postURL(self, url, data=None, *args, **kwargs):
        return self._doURL(self.http_client.postURL, url, data=data, *args, **kwargs)

    def putURL(self, url, data=None, *args, **kwargs):
        return self._doURL(self.http_client.putURL, url, data=data, *args, **kwargs)

    def deleteURL(self, url, params=None, *args, **kwargs):
        return self._doURL(self.http_client.deleteURL, url, params=params, *args, **kwargs)

    def _doURL(self, method, url, params=None, data=None, secure=None, role=None, *args, **kwargs):
        secure = secure or self.secure 
        url = self._prepareURL(url, secure)
        kwargs['role'] = role or self.role
        return method(url, params=params, data=data, *args, **kwargs)

    def get_verbose(self):
        return self.http_client.verbose
    def set_verbose(self, verbose):
        self.http_client.verbose = verbose

    verbose = property(get_verbose, set_verbose)

    def get_max_timeout(self): return self.http_client.max_timeout
    def set_max_timeout(self, timeout): self.http_client.max_timeout = timeout
    max_timeout = property(get_max_timeout, set_max_timeout)

    def get_socket_timeout(self): return self.http_client.socket_timeout
    def set_socket_timeout(self, timeout): self.http_client.socket_timeout = timeout
    socket_timeout = property(get_socket_timeout, set_socket_timeout)

def samweb_method(m):
    """ Attach this function as a method of the SAMWebClient class """
    setattr(SAMWebClient, m.func_name, m)
    return m
