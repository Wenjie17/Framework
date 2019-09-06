import copy
import numpy as np
import pandas as pd
from Engine.Defaults import *
import Core.IO as IO

def AnnualizationFactor(period, annualization):
    if annualization is None:
        try:
            factor = ANNUALIZATION_FACTORS[period]
        except KeyError:
            raise ValueError(
                "Period cannot be '{}'. "
                "Can be '{}'.".format(
                    period, "', '".join(ANNUALIZATION_FACTORS.keys())
                )
            )
    else:
        factor = annualization
    return factor


def CumulativeReturns(returns, starting_value=0, out=None):
    if len(returns) < 1:
        return returns.copy()

    nanmask = np.isnan(returns)
    if np.any(nanmask):
        returns = returns.copy()
        returns[nanmask] = 0

    allocated_output = out is None
    if allocated_output:
        out = np.empty_like(returns)

    np.add(returns, 1, out=out)
    out.cumprod(axis=0, out=out)

    if starting_value == 0:
        np.subtract(out, 1, out=out)
    else:
        np.multiply(out, starting_value, out=out)

    if allocated_output:
        if returns.ndim == 1 and isinstance(returns, pd.Series):
            out = pd.Series(out, index=returns.index)
        elif isinstance(returns, pd.DataFrame):
            out = pd.DataFrame(
                out, index=returns.index, columns=returns.columns,
            )

    return out


def CumulativeReturnsFinal(returns, starting_value=0):
    if len(returns) == 0:
        return np.nan

    if isinstance(returns, pd.DataFrame):
        result = (returns + 1).prod()
    else:
        result = np.nanprod(returns + 1, axis=0)

    if starting_value == 0:
        result -= 1
    else:
        result *= starting_value

    return result


def AnnualVolatility(returns,
                      period=DAILY,
                      alpha=2.0,
                      annualization=None,
                      out=None):

    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    returns_1d = returns.ndim == 1

    if len(returns) < 2:
        out[()] = np.nan
        if returns_1d:
            out = out.item()
        return out

    returns_array = np.asanyarray(returns)

    ann_factor = AnnualizationFactor(period, annualization)
    np.std(returns_array, ddof=1, axis=0, out=out)
    out = np.multiply(out, ann_factor ** (1.0 / alpha), out=out)
    if returns_1d:
        out = out.item()
    return out


def AnnualReturn(returns, period=DAILY, annualization=None):

    if len(returns) < 1:
        return np.nan

    ann_factor = AnnualizationFactor(period, annualization)
    num_years = len(returns) / ann_factor
    # Pass array to ensure index -1 looks up successfully.
    ending_value = CumulativeReturnsFinal(returns, starting_value=1)

    return ending_value ** (1 / num_years) - 1


def MaxDrawdown(returns, out=None):
    """
    See https://en.wikipedia.org/wiki/Drawdown_(economics) for more details.
    """
    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    returns_1d = returns.ndim == 1

    if len(returns) < 1:
        out[()] = np.nan
        if returns_1d:
            out = out.item()
        return out

    returns_array = np.asanyarray(returns)

    cumulative = np.empty(
        (returns.shape[0] + 1,) + returns.shape[1:],
        dtype='float64',
    )
    cumulative[0] = start = 100
    CumulativeReturns(returns_array, starting_value=start, out=cumulative[1:])

    max_return = np.fmax.accumulate(cumulative, axis=0)
    np.min((cumulative - max_return) / max_return, axis=0, out=out)

    if returns_1d:
        out = out.item()
    elif allocated_output and isinstance(returns, pd.DataFrame):
        out = pd.Series(out)

    return out


def SharpeRatio(returns,
                 risk_free=0,
                 period=DAILY,
                 annualization=None,
                 out=None):
    """
    See https://en.wikipedia.org/wiki/Sharpe_ratio for more details.

    """
    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    return_1d = returns.ndim == 1

    if len(returns) < 2:
        out[()] = np.nan
        if return_1d:
            out = out.item()
        return out

    # returns_risk_adj = np.asanyarray(_adjust_returns(returns, risk_free))
    returns_risk_adj = np.asanyarray(returns - risk_free)
    ann_factor = AnnualizationFactor(period, annualization)

    #a = np.mean(returns_risk_adj, axis=0)
    #b = np.std(returns_risk_adj, ddof=1, axis=0)
    #print(a,b)

    np.multiply(
        np.divide(
            np.mean(returns_risk_adj, axis=0),
            np.std(returns_risk_adj, ddof=1, axis=0),
            out=out,
        ),
        np.sqrt(ann_factor),
        out=out,
    )
    if return_1d:
        out = out.item()

    return out


def ExcessSharpe(returns, factor_returns, out=None):
    """
    The excess Sharpe is a simplified Information Ratio that uses
    tracking error rather than "active risk" as the denominator.
    """
    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    returns_1d = returns.ndim == 1

    if len(returns) < 2:
        out[()] = np.nan
        if returns_1d:
            out = out.item()
        return out

    active_return = returns - factor_returns
    tracking_error = np.nan_to_num(np.std(active_return, ddof=1, axis=0))

    out = np.divide(
        np.mean(active_return, axis=0, out=out),
        tracking_error,
        out=out,
    )
    if returns_1d:
        out = out.item()
    return out


class PerformanceTracker(object):
    """
    Tracks the performance of the algorithm.
    """
    def __init__(self, sim_params, trading_calendar, trading_enviroment, portfolio):
        self.sim_params = sim_params
        self.trading_calendar = trading_calendar
        # self.asset_finder = env.asset_finder
        # self.treasury_curves = trading_env.treasury_curves
        self.trading_enviroment = trading_enviroment

        # ---initlize---
        self.portfolio = portfolio
        self.previous = copy.deepcopy(portfolio)
        self._datetime1 = None
        self._datetime2 = None

        #
        fields = ["DateTime","Cash","Equity","Notional","ProfitLoss","Value","Returns"]
        self._dfPerformances = pd.DataFrame(columns=fields)


    @property
    def Perfomence(self):
        return self._dfPerformances


    # ---Every Bar (Settlement Periods)---
    def UpdatePerformance(self):
        # period_open
        # period_close
        datetiem1 = self.previous.DateTime2
        datetiem2 = self.portfolio.DateTime2

        # Beginning DateTime
        if self._datetime1 == None:
            self._datetime1 = self.portfolio.DateTime2

        # Ending DateTime
        self._datetime2 = self.portfolio.DateTime2

        #
        unitNetValue = 1
        if self.previous == None:
            returns = 0
        else:
            start_value = self.previous.Value
            end_value = self.portfolio.Value
            profitloss = end_value - start_value
            if start_value != 0:
                returns = profitloss / start_value
            else:
                returns = 0
            # self.portfolio.UnitNetValue = self.portfolio.UnitNetValue * (1 + returns)

        # fields = ["DateTime", "Cash", "Equity", "Notional", "ProfitLoss", "Value", "UnitNetValue", "MaxDrawdown"]
        cash = self.portfolio.Cash
        equity = self.portfolio.Equity
        notional = self.portfolio.Notional
        profitLoss = self.portfolio.PositionProfitLoss
        value = self.portfolio.Value
        # unitNetValue = self.portfolio.UnitNetValue

        # ---Add a Entry to Performance df---
        count = len(self._dfPerformances)
        localDateTime = Gadget.ToLocalDateTime(self.portfolio.DateTime2)
        # strDateTime = Gadget.ToDateTimeString(localDateTime)
        self._dfPerformances.loc[count] = [localDateTime.date(), cash, equity, notional, profitLoss, value, returns]
        # print(self._dfPerformances)

        #
        self.previous = copy.deepcopy(self.portfolio)


    # ---Run on Session end---
    def ReturnStatistics(self):

        # ---Load Benchmark Return---
        bmSymbol = self.trading_enviroment.BenchmarkSymbol
        dfBenchmark = IO.LoadBarsAsDataFrame(symbol=bmSymbol,
                                             database=self.trading_enviroment.Database,
                                             datetime1=self._datetime1,datetime2=self._datetime2,
                                             instrumentType="Index")
        dfBenchmark.rename(columns={'Close': 'Benchmark'}, inplace=True)
        # print(dfBenchmark.head())
        # print(self._dfPerformances.head())

        # ---Align---
        self._dfPerformances = pd.merge(self._dfPerformances, dfBenchmark, on='DateTime', how='outer')
        # ---Drop---
        self._dfPerformances = self._dfPerformances.dropna()
        # ---excess return---
        self._dfPerformances["BMReturns"] = self._dfPerformances["Benchmark"] / self._dfPerformances["Benchmark"].shift(1) - 1
        self._dfPerformances["BMReturns"] = self._dfPerformances["BMReturns"].fillna(0)
        self._dfPerformances["ExcessReturns"] = self._dfPerformances["Returns"] - self._dfPerformances["BMReturns"]
        # print(self._dfPerformances)

        # ---Cummulative---
        out = CumulativeReturns(self._dfPerformances["Returns"])
        self._dfPerformances["UnitNetValue"] = 1 + out
        #
        out = CumulativeReturns(self._dfPerformances["BMReturns"])
        self._dfPerformances["Benchmark"] = 1 + out
        #
        out = CumulativeReturns(self._dfPerformances["ExcessReturns"])
        self._dfPerformances["CumExcessReturn"] = out
        #
        returns = self._dfPerformances["Returns"]
        benchmarkReturns = self._dfPerformances["BMReturns"]
        excessReturns = self._dfPerformances["ExcessReturns"]

        # ---Export---
        statistics = {}

        #
        maxdrawdown = MaxDrawdown(returns)
        statistics["MaxDrawdown"] = maxdrawdown

        # algo_volatility
        # benchmark_volatility

        # return volatility
        volatility = AnnualVolatility(returns=returns, period=self.sim_params.DataFrequency)
        # compound return
        annulizedReturn = AnnualReturn(returns=returns, period=self.sim_params.DataFrequency)
        # Sharpe base Compound Return
        sharpe2 = annulizedReturn / volatility
        #
        statistics["AnnualReturn"] = annulizedReturn
        statistics["Volatility"] = volatility
        statistics["Sharpe(Compound Return)"] = sharpe2

        # annual alpha
        annulizedExcessReturn = AnnualReturn(returns=excessReturns, period=self.sim_params.DataFrequency)
        statistics["AnnualExcessReturn"] = annulizedExcessReturn

        # market model --> alpha beta

        # sharpe
        sharpe = SharpeRatio(returns=returns, period=self.sim_params.DataFrequency)
        statistics["Sharpe(Avg Return)"] = sharpe

        # --- backup solution---
        information = SharpeRatio(self._dfPerformances["ExcessReturns"])
        statistics["Information"] = information

        # ---Set DateTime as Index---
        self._dfPerformances.set_index("DateTime", inplace=True)

        #
        return statistics


