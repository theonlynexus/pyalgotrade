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
import pytz


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
        return self.openingTime

    @classmethod
    def getOpeningTime(self):
        """Returns the datetime.time representing the closing time."""
        return self.closingTime


######################################################################
# US

class NASDAQ(MarketSession):
    """NASDAQ market session."""
    timezone = pytz.timezone("US/Eastern")


class NYSE(MarketSession):
    """New York Stock Exchange market session."""
    timezone = pytz.timezone("US/Eastern")


class USEquities(MarketSession):
    """US Equities market session."""
    timezone = pytz.timezone("US/Eastern")


class ICE(MarketSession):
    timezone = pytz.timezone("US/Eastern")  # Fix


class CME(MarketSession):
    timezone = pytz.timezone("US/Central")  # Fix


######################################################################
# South America

class MERVAL(MarketSession):
    """Buenos Aires (Argentina) market session."""
    timezone = pytz.timezone("America/Argentina/Buenos_Aires")


class BOVESPA(MarketSession):
    """BOVESPA (Brazil) market session."""
    timezone = pytz.timezone("America/Sao_Paulo")


######################################################################
# Europe

class FTSE(MarketSession):
    """ London Stock Exchange market session."""
    timezone = pytz.timezone("Europe/London")


######################################################################
# Asia

class TSE(MarketSession):
    """Tokyo Stock Exchange market session."""
    timezone = pytz.timezone("Asia/Tokyo")


class KRX(MarketSession):
    """Korea Stock Exchange market session."""
    timezone = pytz.timezone("Asia/Seoul")
    openingTime = time(9, 0, 0, 0, timezone)
    clsoingTime = time(15, 45, 0, 0, timezone)
