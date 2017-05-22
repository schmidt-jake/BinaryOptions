import pandas as pd
import numpy as np
from ticktock import tick, tock
import quandl
from pandas.tseries.offsets import BDay
from talib import RSI, OBV, MACD, EMA


def macdSig(macd):
	return np.subtract(macd[0], macd[1])


def toPercent(ts):
	last = ts[-1]
	return (last - np.mean(ts)) / last if last > 0. else 0.


def computeData(panel):
	tick()
	QUANDL_API_KEY = 'ZCLSs1s9ojeHEFssKL6S'
	qdata = quandl.get(
		['AAII/AAII_SENTIMENT.1',
		 'AAII/AAII_SENTIMENT.3',
		 'YALE/US_CONF_INDEX_VAL_INDIV.1',
		 'USTREASURY/LONGTERMRATES.1',
		 'USTREASURY/REALYIELD.1',
		 'ML/USTRI.1'],
		start_date='2002-01-03',
		api_key=QUANDL_API_KEY).tz_localize('US/Eastern').resample('H').interpolate(method='time').fillna(method='bfill')
	qdata.columns = ['bull', 'bear', 'conf', 'tr', 'ty', 'by']
	
	tock('pre-format')
	
	# none of the pandas tseries tools fit for this particular analysis, so I'm stuck with slow nested loops
	hours = range(8, 14 + 1)
	days = range(1, 10 + 1)
	l1 = []
	# ~320 sec per loop
	for h in hours:
		l2 = []
		idx = panel.loc[:, (panel.major_axis.hour == h) & (panel.major_axis.dayofweek < 5), :].major_axis
		for d in days:
			print('-', end=' ')
			d1 = {}
			for i in idx:
				i = pd.to_datetime(i)
				prd = panel.loc[:, i.replace(hour=16) - BDay(d):i, :]
				prdP = prd['price']
				prdV = prd['vol']
				tp = len(prdP)
				dic = {
					'p_fwd': panel.loc['price', i + dt.timedelta(hours=2), :],
					'p_fwd_std_15min': panel.loc['price', (panel.major_axis >= i + dt.timedelta(hours=1, minutes=45)) & (panel.major_axis <= i + dt.timedelta(hours=2)), :].std(),
					'p_cur': prdP.iloc[-1],
					'p_prd': prdP.iloc[0],
					'p_ema': [EMA(prdP[j].values, timeperiod=tp - 1)[-1] if not prdP[j].isnull().all() else np.NaN for j in prdP.columns],
					'p_ema2': [EMA(prdP[j].values, timeperiod=tp / 2)[-1] if not prdP[j].isnull().all() else np.NaN for j in prdP.columns],
					'v_ema': [EMA(prdV[j].values, timeperiod=tp - 1)[-1] if not prdV[j].isnull().all() else np.NaN for j in prdV.columns],
					'v_ema2': [EMA(prdV[j].values, timeperiod=tp / 2)[-1] if not prdV[j].isnull().all() else np.NaN for j in prdV.columns],
					'rsi': [RSI(prdP[j].values, timeperiod=tp - 1)[-1] if not prdP[j].isnull().all() else np.NaN for j in prdP.columns],
					'obv': [toPercent(OBV(prdP[j].values, prdV[j].values)) if not (prdP[j].isnull().all()) | (prdV[j].isnull().all()) else np.NaN for j in prdP.columns],
					'macd': [macdSig(MACD(prdP[j].values, slowperiod=tp / 2, fastperiod=tp / 3, signalperiod=tp / 5))[-1] if not prdP[j].isnull().all() else np.NaN for j in prdP.columns],
					'bull': qdata.loc[i, 'bull'],
					'bear': qdata.loc[i, 'bear'],
					'tr': qdata.loc[i, 'tr'],
					'ty': qdata.loc[i, 'ty'],
					'by': qdata.loc[i, 'by'],
					'conf': qdata.loc[i, 'conf'],
				}
				d1[i.normalize()] = pd.DataFrame.from_dict(dic)
			l2.append(pd.Panel.from_dict(d1).swapaxes(0, 1, copy=False))
		l1.append(pd.concat(l2, keys=days, axis=2))
		data = pd.concat(l1, keys=hours)
		data = data.to_frame(filter_observations=False)
		data = data.unstack().stack(level=(0, 1), dropna=False)
		data.index.set_names(['date', 'lkbk', 'time', 'mkt'], inplace=True)
		data.reset_index(inplace=True)
		data['wkdy'] = data['date'].map(lambda j: j.weekday())
		data['wk#'] = data['date'].map(lambda j: int(round(j.day - 1) / 7))
		data['prd_rtn'] = (data['p_cur'] - data['p_prd']) / data['p_cur']
		data['fwd_rtn'] = (data['p_fwd'] - data['p_cur']) / data['p_cur']
		data['diff'] = (2 * data['p_cur'] - data['p_ema'] - data['p_ema2']) / data['p_cur']
		data['v_diff'] = (2 * data['v_ema2'] - data['v_ema'] - data['v_ema2']) / data['v_ema2']
		data['bull-bear'] = data['bull'] - data['bear']
		data['s'] = (data['prd_rtn'] > 0.0).astype(int)
		data['w'] = (data['fwd_rtn'] >= 0.0).astype(int)
		data.set_index('date', inplace=True)
		tock(h)
	return data


def panelizeRawData(raw_price_path, raw_volume_path, panel_path):
	# combines, cleans, and pre-formats the raw price and volume .txt files.
	p = pd.read_table(raw_price_path, delim_whitespace=True, parse_dates=[[0, 1]], header=None, index_col='0_1').dropna(how='all')
	v = pd.read_table(raw_volume_path, delim_whitespace=True, parse_dates=[[0, 1]], header=None, index_col='0_1').dropna(how='all')
	p.index = pd.to_datetime(p.index).tz_localize('UTC').tz_convert('US/Eastern')
	v.index = pd.to_datetime(v.index).tz_localize('UTC').tz_convert('US/Eastern')
	pan = pd.Panel.from_dict({'price': p, 'vol': v})
	pan.minor_axis = [0, 1, 2, 3]
	pan.major_axis.name = None
	pd.to_pickle(pan, panel_path)
