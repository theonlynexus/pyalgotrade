# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from datetime import time
from enum import Enum
import pytz


class Weekdays(Enum):
    Su = 0 Sun = 0 Sunday = 0
    Mo = 1 Mon = 1 Monday = 1
    Tu = 2 Tue = 2 Tuesday = 2
    We = 3 Wed = 3 Wednesday = 3
    Th = 4 Thu = 4 Thursday = 4
    Fr = 5 Fri = 5 Friday = 5
    Sa = 6 Sat = 6 Saturday = 6


class WeekdayTime(object):
    def __init__(self):
        self.time = time(0, 0, 0)
        self.weekday = Weekdays.Mo


class DailySession(object):
    def __init__(self, start, end):
        if start is None or end is None:
            raise ValueError('Start/End is None')
        self.start = start
        self.end = end
        invertedWeekdays = end.weekday < start.weekday
        invertedTimes = end.weekday <= start.weekday and end.time < start.time
        if invertedWeekdays:
            raise ValueError('End weekday is earlier than start weekday')
        elif invertedTimes:
            raise ValueError('End time is earlier than start time')


# http://en.wikipedia.org/wiki/List_of_market_opening_times
class MarketSession(object):
    """Base class for market sessions.

    .. note::
        This is a base class and should not be used directly.
    """

    @classmethod
    def getTimezone(self):
        """Returns the pytz timezone for the market session."""
        return self.timezone

    @classmethod
    def getOpeningTime(self):
        """Returns the datetime.time representing the opening time."""
        return self.sessions

######################################################################
# US


class NASDAQ(MarketSession):
    """NASDAQ market session."""
    timezone = pytz.timezone("US/Eastern")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(16, tzinfo=timezone)))
    ]


class NYSE(MarketSession):
    """New York Stock Exchange market session."""
    timezone = pytz.timezone("US/Eastern")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(16, tzinfo=timezone)))
    ]


class ICE(MarketSession):
    # https://www.theice.com/productguide/Search.shtml?tradingHours=
    # Trading times for ICE can be computed either as London based or
    # US Eastern (NY) based. Rounding all minutes to zero (sessions
    # shorter by 10 minutes, except on Sunday).
    timezone = pytz.timezone("US/Eastern")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Su, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(18, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Mo, time(19, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(18, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(19, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(18, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(19, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(18, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(19, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(18, tzinfo=timezone)))
    ]


class CME(MarketSession):
    # http://www.cmegroup.com/trading-hours.html#energy
    # Trading times for CME can be computed either US Eastern or
    # US Central based. Energy products are traded for the longest
    # time (23hrs/day) whereas agricultural products have shorter trading
    # sessions that will need to be specified in the products themselves.
    timezone = pytz.timezone("US/Central")  # Fix
    sessions = [
        DailySession(WeekdayTime(Weekdays.Su, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Mo, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(16, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(17, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(16, tzinfo=timezone)))
    ]


######################################################################
# South America

class BCBA(MarketSession):
    """Buenos Aires (Argentina) market session."""
    timezone = pytz.timezone("America/Argentina/Buenos_Aires")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(11, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(17, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(11, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(17, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(11, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(17, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(11, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(17, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(11, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(17, tzinfo=timezone)))
    ]


class BOVESPA(MarketSession):
    """BOVESPA (Brazil) market session."""
    timezone = pytz.timezone("America/Sao_Paulo")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(10, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(17, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(10, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(17, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(10, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(17, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(10, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(17, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(10, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(17, 30, tzinfo=timezone)))
    ]


######################################################################
# Europe

class LSE(MarketSession):
    """ London Stock Exchange market session."""
    timezone = pytz.timezone("Europe/London")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(8, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(16, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(8, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(16, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(8, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(16, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(8, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(16, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(8, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(16, 30, tzinfo=timezone)))
    ]


######################################################################
# Asia

class TSE(MarketSession):
    """Tokyo Stock Exchange market session."""
    timezone = pytz.timezone("Asia/Tokyo")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(15, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(15, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(15, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(15, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(15, tzinfo=timezone)))
    ]


class KRX(MarketSession):
    """Korea Stock Exchange market session."""
    timezone = pytz.timezone("Asia/Seoul")
    sessions = [
        DailySession(WeekdayTime(Weekdays.Mo, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Mo, time(15, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Tu, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Tu, time(15, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.We, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.We, time(15, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Th, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Th, time(15, 30, tzinfo=timezone))),
        DailySession(WeekdayTime(Weekdays.Fr, time(9, tzinfo=timezone)),
                     WeekdayTime(Weekdays.Fr, time(15, 30, tzinfo=timezone)))
    ]
