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
.. moduleauthor:: Massimo Fierro <massimo.fierro@gmail.com>
"""

from pyalgotrade import stratanalyzer
from pyalgotrade import broker

from pyalgotrade.stratanalyzer import trades
from pyalgotrade.stratanalyzer import returns
from pyalgotrade.stratanalyzer.extendedpositiontracker import ExtendedPositionTracker

import numpy as np


class ExtendedTradesAnalyzer(trades.Trades):
    """
    An extended :class:`trades.Trades` that in addition to profits
    also records entry/exit prices and dates, as well as the number
    of contracts/shares hold and whether the position was a long or
    short.

    .. note::
        Like the base class this analyzer operates on individual
        completed trades.
    """

    def __init__(self):
        super(ExtendedTradesAnalyzer, self).__init__()
        self.allEnterDates = []
        self.allExitDates = []
        self.allLongFlags = []
        self.allEntryPrices = []
        self.allExitPrices = []
        self.allContracts = []
        self.allRunups = []
        self.allDrawDowns = []
        self.allEntryEff = []
        self.allExitEff = []
        self.allTotalEff = []
        self.allEquity = []
        self.pnlDict = {}
        self.cumPnlDict = {}
        self.openPosition = None
        self.initialEquity = None
        self.cumPnl = 0

    def _updateTrades(self, posTracker):
        self.openPosition = posTracker

        # The price doesn't matter since the position should be closed.
        price = 0
        assert posTracker.getPosition() == 0
        netProfit = posTracker.getPnL(price)
        netReturn = posTracker.getReturn(price)

        if netProfit > 0:
            self._Trades__profits.append(netProfit)
            self._Trades__positiveReturns.append(netReturn)
            self._Trades__profitableCommissions.append(
                posTracker.getCommissions())
        elif netProfit < 0:
            self._Trades__losses.append(netProfit)
            self._Trades__negativeReturns.append(netReturn)
            self._Trades__unprofitableCommissions.append(
                posTracker.getCommissions())
        else:
            self._Trades__evenTrades += 1
            self._Trades__evenCommissions.append(posTracker.getCommissions())

        low = posTracker._low
        high = posTracker._high
        if posTracker.isLong:
            runup = max(high - posTracker.entryPrice, netProfit)
        else:
            runup = max(posTracker.entryPrice - low, netProfit)
        if posTracker.isLong:
            drawdown = min(low - posTracker.entryPrice, netProfit)
        else:
            drawdown = min(posTracker.entryPrice - high, netProfit)
        priceRange = high - low

        if posTracker.isLong:
            entryEff = 1 - (posTracker.entryPrice - low) / priceRange
            exitEff = (posTracker.exitPrice - low) / priceRange
        else:
            entryEff = (posTracker.entryPrice - low) / priceRange
            exitEff = 1 - (posTracker.exitPrice - low) / priceRange
        totalEff = netProfit / priceRange

        for x in [entryEff, exitEff, totalEff]:
            if x > 100:
                x = 100
            elif x < -100:
                x = -100

        self._Trades__all.append(netProfit)
        self._Trades__allReturns.append(netReturn)
        self._Trades__allCommissions.append(posTracker.getCommissions())
        self.allEnterDates.append(posTracker.entryDate)
        self.allExitDates.append(posTracker.exitDate)
        self.allLongFlags.append(posTracker.isLong)
        self.allEntryPrices.append(posTracker.entryPrice)
        self.allExitPrices.append(posTracker.exitPrice)
        self.allContracts.append(posTracker.contracts)
        self.allRunups.append(runup)
        self.allDrawDowns.append(drawdown)
        self.allEntryEff.append(entryEff)
        self.allExitEff.append(exitEff)
        self.allTotalEff.append(totalEff)

        posTracker.reset()

    def __updatePosTracker_impl(self, posTracker, price, commission, quantity,
                                datetime):
        currentShares = posTracker.getPosition()

        if currentShares > 0:  # Current position is long
            if quantity > 0:  # Increase long position
                posTracker.buy(quantity, price, commission)
            else:
                newShares = currentShares + quantity
                if newShares == 0:  # Exit long.
                    posTracker.sell(currentShares, price, commission)
                    posTracker.exitDate = datetime
                    self._updateTrades(posTracker)
                elif newShares > 0:  # Sell some shares.
                    posTracker.sell(quantity * -1, price, commission)
                else:
                    # Exit long and enter short. Use proportional commissions.
                    proportionalCommission = commission * \
                        currentShares / float(quantity * -1)
                    posTracker.sell(currentShares, price,
                                    proportionalCommission)
                    posTracker.exitDate = datetime
                    self._updateTrades(posTracker)
                    proportionalCommission = commission * \
                        newShares / float(quantity)
                    posTracker.sell(newShares * -1, price,
                                    proportionalCommission)
                    posTracker.entryDate = datetime
        elif currentShares < 0:  # Current position is short
            if quantity < 0:  # Increase short position
                posTracker.sell(quantity * -1, price, commission)
            else:
                newShares = currentShares + quantity
                if newShares == 0:  # Exit short.
                    posTracker.buy(currentShares * -1, price, commission)
                    posTracker.exitDate = datetime
                    self._updateTrades(posTracker)
                elif newShares < 0:  # Re-buy some shares.
                    posTracker.buy(quantity, price, commission)
                else:
                    # Exit short and enter long. Use proportional commissions.
                    proportionalCommission = (
                        commission * currentShares * -1 / float(quantity))
                    posTracker.buy(currentShares * -1, price,
                                   proportionalCommission)
                    posTracker.exitDate = datetime
                    self._updateTrades(posTracker)
                    proportionalCommission = commission * \
                        newShares / float(quantity)
                    posTracker.buy(newShares, price, proportionalCommission)
                    posTracker.entryDate = datetime
        elif quantity > 0:
            posTracker.buy(quantity, price, commission)
            posTracker.entryDate = datetime
        else:
            posTracker.sell(quantity * -1, price, commission)
            posTracker.entryDate = datetime

    def _updatePosTracker(self, posTracker, price, commission, quantity,
                          datetime):
        self.__updatePosTracker_impl(posTracker, price, commission, quantity,
                                     datetime)

    def _onOrderEvent(self, broker_, orderEvent):
        # Only interested in filled or partially filled orders.
        if orderEvent.getEventType() not in (
                broker.OrderEvent.Type.PARTIALLY_FILLED,
                broker.OrderEvent.Type.FILLED):
            return

        order = orderEvent.getOrder()

        # Get or create the tracker for this instrument.
        try:
            posTracker = self._Trades__posTrackers[order.getInstrument()]
        except KeyError:
            posTracker = ExtendedPositionTracker(order.getInstrumentTraits())
            self._Trades__posTrackers[order.getInstrument()] = posTracker

        # Update the tracker for this order.
        execInfo = orderEvent.getEventInfo()
        price = execInfo.getPrice()
        commission = execInfo.getCommission()
        action = order.getAction()
        if action in [broker.Order.Action.BUY,
                      broker.Order.Action.BUY_TO_COVER]:
            quantity = execInfo.getQuantity()
        elif action in [broker.Order.Action.SELL,
                        broker.Order.Action.SELL_SHORT]:
            quantity = execInfo.getQuantity() * -1
        else:  # Unknown action
            assert(False)

        # This must be done before updatind the tracker, or the PnL
        # will be zero if we exit the position
        pnl = posTracker.getPnL(price)

        self._updatePosTracker(posTracker, price, commission,
                               quantity, execInfo.getDateTime())

        if posTracker.getPosition() == 0:
            self.cumPnl += pnl
            self.cumPnl -= self.lastPnl
            self.cumPnlDict[execInfo.getDateTime()] = self.cumPnl
            self.lastPnl = 0

    def attached(self, strat):
        strat.getBroker().getOrderUpdatedEvent().subscribe(self._onOrderEvent)

    def __beforeOnBars_impl(self, strat, bars):
        self._bars = bars

        for instrument in bars.keys():
            try:
                posTracker = self._Trades__posTrackers[instrument]
            except KeyError:
                traits = strat.getBroker().getInstrumentTraits(instrument)
                posTracker = ExtendedPositionTracker(traits)
                self._Trades__posTrackers[instrument] = posTracker
                continue

            if self.initialEquity is None:
                self.lastPnl = 0
                self.initialEquity = strat.getBroker().getCash()
                self.allEquity.append(self.initialEquity)

            if posTracker is not None and posTracker.getPosition() != 0:
                pnl = posTracker.getPnL(bars[instrument].getClose())
                self.pnlDict[bars.getDateTime()] = pnl
                if pnl != 0:
                    self.cumPnl += pnl
                    self.cumPnl -= self.lastPnl
                    self.lastPnl = pnl
                # else:
                #     self.cumPnl -= self.lastPnl
                #     self.lastPnl = 0
            self.cumPnlDict[bars.getDateTime()] = self.cumPnl

    def beforeOnBars(self, strat, bars):
        self.__updateLowsAndHighs(strat, bars)
        self.__beforeOnBars_impl(strat, bars)

    def afterOnBars(self, strat, bars):
        self.__updateLowsAndHighs(strat, bars)

    def __updateLowsAndHighs(self, strat, bars):
        for instrument in bars.keys():
            try:
                posTracker = self._Trades__posTrackers[instrument]
            except KeyError:
                traits = strat.getBroker().getInstrumentTraits(instrument)
                posTracker = ExtendedPositionTracker(traits)
                self._Trades__posTrackers[instrument] = posTracker
                continue

            high = bars[instrument].getHigh()
            low = bars[instrument].getLow()
            close = bars[instrument].getClose()
            posTracker.checkAndSetHigh(high)
            posTracker.checkAndSetLow(low)
