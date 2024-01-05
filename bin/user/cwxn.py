# Copyright 2014-2020 Matthew Wall
# Distributed under terms of the GPLv3
"""
Emit loop data to wxnow.txt file with Cumulus format

The Cumulus wxnow.txt file format is detailed in the Cumulus Wiki:
    https://cumuluswiki.org/a/Wxnow.txt

Put this file in the weewx 'user' directory, then add the following to the
weewx configuration file:

[CumulusWXNow]
    filename = /path/to/wxnow.txt

[Engine]
    [[Services]]
        process_services = ..., user.cwxn.CumulusWXNow
"""

# FIXME: when value is None, we insert a 0.  but is there something in the
#        aprs spec that is more appropriate?

import time

import weewx
import weewx.wxformulas
import weeutil.weeutil
import weeutil.Sun
from weewx.engine import StdService

try:
    # WeeWX4 logging
    import logging
    from weeutil.logger import log_traceback

    log = logging.getLogger(__name__)

    def logdbg(msg):
        log.debug(msg)

    def loginf(msg):
        log.info(msg)

    def logerr(msg):
        log.error(msg)

    def log_traceback_error(prefix=''):
        log_traceback(log.error, prefix=prefix)

except ImportError:
    # WeeWX legacy (v3) logging via syslog
    import syslog
    from weeutil.weeutil import log_traceback

    def logmsg(level, msg):
        syslog.syslog(level, 'cwxn: %s' % msg)

    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)

    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)

    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

    def log_traceback_error(prefix=''):
        log_traceback(prefix=prefix, loglevel=syslog.LOG_ERR)

VERSION = "0.5"

if weewx.__version__ < "3":
    raise weewx.UnsupportedFeature("WeeWX 3 is required, found %s" %
                                   weewx.__version__)


def convert(v, metric, group, from_unit_system, to_units):
    ut = weewx.units.getStandardUnitType(from_unit_system, metric)
    vt = (v, ut[0], group)
    v = weewx.units.convert(vt, to_units)[0]
    return v


def nullproof(key, data):
    if key in data and data[key] is not None:
        return data[key]
    return 0


def calcRainHour(dbm, ts):
    sts = ts - 3600
    val = dbm.getSql("SELECT SUM(rain) FROM %s "
                     "WHERE dateTime>? AND dateTime<=?" % dbm.table_name,
                     (sts, ts))
    if val is None:
        return None
    return val[0]


def calcRain24(dbm, ts):
    sts = ts - 86400
    val = dbm.getSql("SELECT SUM(rain) FROM %s "
                     "WHERE dateTime>? AND dateTime<=?" % dbm.table_name,
                     (sts, ts))
    if val is None:
        return None
    return val[0]


def calcDayRain(dbm, ts):
    sts = weeutil.weeutil.startOfDay(ts)
    val = dbm.getSql("SELECT SUM(rain) FROM %s "
                     "WHERE dateTime>? AND dateTime<=?" % dbm.table_name,
                     (sts, ts))
    if val is None:
        return None
    return val[0]


class CumulusWXNow(StdService):

    def __init__(self, engine, config_dict):
        super(CumulusWXNow, self).__init__(engine, config_dict)
        loginf("service version is %s" % VERSION)
        d = config_dict.get('CumulusWXNow', {})
        self.filename = d.get('filename', '/var/tmp/wxnow.txt')
        binding = d.get('binding', 'loop').lower()
        if binding == 'loop':
            self.bind(weewx.NEW_LOOP_PACKET, self.handle_new_loop)
        else:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.handle_new_archive)

        loginf("binding is %s" % binding)
        loginf("output goes to %s" % self.filename)

    def handle_new_loop(self, event):
        self.handle_data(event.packet)

    def handle_new_archive(self, event):
        self.handle_data(event.record)

    def handle_data(self, event_data):
        try:
            dbm = self.engine.db_binder.get_manager('wx_binding')
            data = self.calculate(event_data, dbm)
            self.write_data(data)
        except Exception as e:
            log_traceback_error('cwxn: **** ')

    def calculate(self, packet, archive):
        pu = packet.get('usUnits')
        data = dict()
        data['dateTime'] = packet['dateTime']

        # Wind direction calculations
        if 'windDir' in packet and packet['windDir'] is not None:
            data['windDir'] = ("%03d" % int(packet['windDir']))
        else:
            data['windDir'] = "   "

        # Wind speed calculations
        if 'windSpeed' in packet and packet['windSpeed'] is not None:
            data['windSpeed'] = ("/%03d" % int( convert(packet['windspeed'], 'windSpeed', 'group_speed', pu, 'mile_per_hour')))
        else:
            data['windSpeed'] = "/   "

        # Wind gust calculations
        if 'windGust' in packet and packet['windGust'] is not None:
            data['windGust'] = ("g%03d" % int( convert(packet['windgust'], 'windGust', 'group_speed', pu, 'mile_per_hour')))
        else:
            data['windGust'] = "g   "

        # Temperature calculations
        if 'outTemp' in packet and packet['outTemp'] is not None:
            data['outTemp'] = ("t%03d" % int( convert(packet['outTemp'], 'outTemp', 'group_temperature', pu, 'degree_F')))
        else:
            data['outTemp'] = "t   "

        # Humidity calculations
        if 'outHumidity' in packet and packet['outHumidity'] is not None:
            data['outHumidity'] = ("h%02d" % int(packet['outHumidity']))
        else:
            data['outHumidity'] = "h   "

        # Barometer calculations
        if 'barometer' in packet and packet['barometer'] is not None:
            data['barometer'] = ("b%05d" % int( convert(packet['barometer'], 'pressure', 'group_pressure', pu, 'mbar') * 10))
        else:
            data['barometer'] = 'b     '


        v = calcRainHour(archive, data['dateTime'])
        if v is None:
            v = 0
        data['hourRain'] = convert(v, 'rain', 'group_rain', pu, 'inch')

        if 'rain24' in packet:
            v = nullproof('rain24', packet)
        else:
            v = calcRain24(archive, data['dateTime'])
            v = 0 if v is None else v
        data['rain24'] = convert(v, 'rain', 'group_rain', pu, 'inch')

        if 'dayRain' in packet:
            v = nullproof('dayRain', packet)
        else:
            v = calcDayRain(archive, data['dateTime'])
            v = 0 if v is None else v
        data['dayRain'] = convert(v, 'rain', 'group_rain', pu, 'inch')


        return data

    def write_data(self, data):
        fields = []
        fields.append(data['windDir'])
        fields.append(data['windSpeed'])
        fields.append(data['windGust'])
        fields.append(data['outTemp'])
        fields.append("r   ") # fields.append("r%03d" % int(data['hourRain'] * 100))
        fields.append("p   ") # fields.append("p%03d" % int(data['rain24'] * 100))
        fields.append("P   ") # fields.append("P%03d" % int(data['dayRain'] * 100))
        fields.append(data['outHumidity'])
        fields.append(data['barometer'])

        with open(self.filename, 'w') as f:
            f.write(time.strftime("%b %d %Y %H:%M\n",
                                  time.localtime(data['dateTime'])))
            f.write(''.join(fields))
            f.write("\n")
