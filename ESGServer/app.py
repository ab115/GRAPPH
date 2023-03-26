import yahooquery as yfinq
from flask import Flask, jsonify, make_response
import csv
from flask import request
import json
import pandas as pd
import numpy as np
import sys  
sys.path.append('./src')
from edge import *
from edge_risk_kit import *
import edge_risk_kit as erk
from tqdm.notebook import tqdm
import yahooquery as yf
import jsons

#import seaborn as sns
#import matplotlib.pyplot as plt

app = Flask(__name__)
#cors = CORS(app, resources={r"/*": {"origins": "*"}})

DATA_FOLDER = 'C:\\Pinaki\\Work\\Code\\ESGServer\\.venv\\'

class GraphData:
        gDataBM = []
        gDataPF =[]
        gLabels = []
  
        def __init__(self, pLabels, pDataBM, pDataPF):
                self.gLabels = pLabels
                self.gDataBM = pDataBM
                self.gDataPF = pDataPF


@app.get('/get_esg_scores')
def get_esg_scores():
        csvfile = open('C:\Pinaki\Work\Code\ESGServer\.venv\esg_scores.csv', 'r')
        jsonfile = open('esg_scores.json', 'w')

        fieldnames = ("symbol", "socialScore", "governanceScore", "environmentScore", "totalEsg", "esgPerformance", "percentile", "peerGroup", "highestControversy")
        reader = csv.DictReader(csvfile, fieldnames)
        
        rowcount = 0
        jsonfile.write('[')
        for row in reader:
                if rowcount == 0:
                        rowcount = rowcount + 1
                        continue
                else:
                        json.dump(row, jsonfile, sort_keys=True, indent=4, separators=(',', ':'))                        
                        jsonfile.write(',')
                        jsonfile.write('\n')

        csvfile.close()
        jsonfile.write("{} ]")
        jsonfile.close()
        jsonfile = open('esg_scores.json')

        # returns JSON object as
        # a dictionary
        data = json.load(jsonfile)
              
        response = make_response(data)
        response.headers['Access-Control-Allow-Origin'] = '*' 
        response.content_type = 'application/json'
        return response, 200

@app.get('/get_portfolio')
def get_portfolio():
        csvfile = open('C:\Pinaki\Work\Code\ESGServer\.venv\Portfolio.csv', 'r')
        jsonfile = open('Portfolio.json', 'w')

        fieldnames = ("Symbol", "Name", "Sector", "CurrentPrice", "ClosedPrice")
        reader = csv.DictReader(csvfile, fieldnames)
        
        rowcount = 0
        jsonfile.write('[')
        for row in reader:
                if rowcount == 0:
                        rowcount = rowcount + 1
                        continue
                else:
                        json.dump(row, jsonfile, sort_keys=True, indent=4, separators=(',', ':'))                        
                        jsonfile.write(',')
                        jsonfile.write('\n')

        csvfile.close()
        jsonfile.write("{} ]")
        jsonfile.close()
        jsonfile = open('Portfolio.json')

        # returns JSON object as
        # a dictionary
        data = json.load(jsonfile)
              
        response = make_response(data)
        response.headers['Access-Control-Allow-Origin'] = '*' 
        response.content_type = 'application/json'
        return response, 200

@app.route('/fetch_esg_scores', methods=['GET'])
def fetch_esg_scores():
        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        snp.head()
        esg_data = pd.DataFrame([])
        for ticker in tqdm(snp.index):

                try:
                        print('Processing {}'.format(ticker))
                        temp = yfinq.Ticker(ticker).esg_scores
                        tempdf = pd.DataFrame.from_dict(temp).T
                        tempdf['symbol'] = str(ticker)
                        esg_data = pd.concat([esg_data, tempdf])
                except Exception as e:
                        print(e)
                        continue
        esg_data.set_index('symbol', inplace=True)
        required_cols = ['socialScore', 'governanceScore', 'environmentScore', 'totalEsg',
                                 'esgPerformance', 'percentile', 'peerGroup', 'highestControversy']
        esg_data.columns.name = ''
        esg_data = esg_data[required_cols]
        esg_data = esg_data.apply(pd.to_numeric, errors='ignore')
        esg_data.shape
        esg_data.sort_values('totalEsg', ascending=False).head()
        esg_data.to_csv(DATA_FOLDER + 'esg_scores_13.csv')

        return jsonify("Success fetch esg scores"), 200

@app.route('/get_security_data', methods=['GET'])
def get_security_data():
        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        snp.head()
        tickers = snp.index.to_list()

        security_data = pd.DataFrame([])
        for ticker in tqdm(tickers):
                try:
                        info = yfinq.Ticker(ticker).asset_profile
                        summary = yfinq.Ticker(ticker).summary_detail
                        infodf = pd.DataFrame.from_dict(info).T
                        summarydf = pd.DataFrame.from_dict(summary).T
                        infodf['symbol'] = ticker
                        summarydf['symbol'] = ticker
                        stockdf = pd.merge(infodf, summarydf, on="symbol")
                        stockdf['symbol'] = ticker
                        security_data = pd.concat([security_data, stockdf])

                except Exception as e:
                        print(e)
                        continue

        security_data.shape
        security_data.set_index('symbol', inplace=True)
        security_data.to_csv(DATA_FOLDER + 'security_data.csv')
        return jsonify("Success fetch security scores"), 200

@app.route('/get_security_prices', methods=['GET'])
def get_security_prices():
        start_date = '2022-01-01'
        end_date = '2023-02-28'
        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        tickers = snp.index.to_list()
        data =yf.download(tickers, start=start_date, end=end_date)
        prices = data['Adj Close'][tickers]
        px = prices.loc['2015':].dropna(axis=1, how='all')
        px.to_csv(DATA_FOLDER + 'prices.csv')

        return jsonify("Success building prices"), 200

@app.route('/do_analytics', methods=['GET'])
def do_analytics():        
        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        px = pd.read_csv(DATA_FOLDER + 'prices.csv')
        px.Date = pd.to_datetime(px.Date)
        px.set_index('Date', inplace=True)
        px = px[px.columns[px.count() == px.count().max()]]
        esg_data = pd.read_csv(DATA_FOLDER + 'esg_scores_13.csv')
        security_data = pd.read_csv(DATA_FOLDER + 'security_data.csv')
        esg_data.set_index('symbol', inplace=True)
        security_data.set_index('symbol', inplace=True)
        rets_monthly, cov_monthly = calcRetsCov(px, 'M')
        rets_period = rets_monthly
        
        PERIODS_PER_YEAR = 12
        RISK_FREE_RATE = 0.013
        risk_data = erk.summary_stats(rets_period, riskfree_rate=RISK_FREE_RATE, periods_per_year=PERIODS_PER_YEAR).sort_values('Sharpe Ratio', ascending=False)
        full_data = risk_data.join(esg_data).join(security_data['marketCap']).join(snp)
        full_data = full_data[~full_data.totalEsg.isnull()]
        full_data['mktcap_grp'] = pd.cut(full_data.percentile, 3, labels=["Small", "Medium", "Large"])
        largePeerGroup = esg_data.peerGroup.value_counts().index[0:20].to_list()
        full_data['peerGroup2'] = full_data.peerGroup.apply(lambda x: x if x in largePeerGroup else 'Others')
        full_data['esg_soc_grp'] = pd.cut(full_data.socialScore, 5,
                                          labels=["Severe Risk", "High Risk", "Medium Risk", "Low Risk", "No Risk"])
        full_data['esg_env_grp'] = pd.cut(full_data.environmentScore, 5,
                                          labels=["Severe Risk", "High Risk", "Medium Risk", "Low Risk", "No Risk"])
        full_data['esg_gov_grp'] = pd.cut(full_data.governanceScore, 5,
                                          labels=["Severe Risk", "High Risk", "Medium Risk", "Low Risk", "No Risk"])
        full_data['esg_tot_grp'] = pd.cut(full_data.totalEsg, 5,
                                          labels=["Severe Risk", "High Risk", "Medium Risk", "Low Risk", "No Risk"])        
        full_data.shape
        #print(full_data)
        return full_data



def calcRetsCov(px, freq):
        px_freq = px.resample(freq).fillna('ffill')
        px_freq.index = px_freq.index.to_period(freq)
        rets = px_freq.pct_change().dropna(axis=1, how='all').dropna()
        cov = rets.cov()
        return rets, cov

@app.get('/do_panalysis')
def do_panalysis():
        full_data = do_analytics()

        score_list = ['socialScore', 'governanceScore', 'environmentScore', 'totalEsg']
        num_of_stocks = 30

        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        px = pd.read_csv(DATA_FOLDER + 'prices.csv')
        px.Date = pd.to_datetime(px.Date)
        px.set_index('Date', inplace=True)
        px = px[px.columns[px.count() == px.count().max()]]

        rets_monthly, cov_monthly = calcRetsCov(px, 'M')

        er_bmk = erk.annualize_rets(rets_monthly, 12)
        cov_bmk = rets_monthly.cov()

        return_bmk = (ew(er_bmk) * rets_monthly).sum(axis=1)
        wealth_bmk = erk.drawdown(return_bmk).Wealth

        return_port = {}
        wealth_port = {}

        return_port['bmk'] = return_bmk
        wealth_port['bmk'] = wealth_bmk

        for score in score_list:
                return_ = {}
                wealth_ = {}

                stock_selected = full_data.sort_values(score, ascending=False).head(num_of_stocks).index

                ## only need the expected return to generate the equal weights... other than that is redundant for now
                er_port = erk.annualize_rets(rets_monthly[stock_selected], 12)
                cov_port = rets_monthly[stock_selected].cov()

                return_ = (ew(er_port) * rets_monthly[stock_selected]).sum(axis=1)
                wealth_ = erk.drawdown(return_).Wealth

                return_port[score] = return_
                wealth_port[score] = wealth_

        return_port = pd.DataFrame(return_port)
        wealth_port = pd.DataFrame(wealth_port)
        
        
        df = pd.DataFrame()
        df['totalEsg'] = return_port.get("totalEsg")
        df['bmk'] = return_port.get("bmk")
        df['Date1'] = return_port.index.values
        df['labels'] = df['Date1'].dt.month.astype(str) + "-" + df['Date1'].dt.year.astype(str)
        
        print (df)
        data = GraphData(df.get('labels'), df.get("bmk"), df.get("totalEsg"))
        
        response = make_response(jsons.dumps(data))
        response.headers['Access-Control-Allow-Origin'] = '*' 
        response.content_type = 'application/json'
        return response, 200
 
@app.get('/portfolioVsBenchmark')
def portfolioVsBenchmark():
        full_data = do_analytics()

        score_list = ['socialScore', 'governanceScore', 'environmentScore', 'totalEsg']
        num_of_stocks = 30

        snp = pd.read_csv(DATA_FOLDER + 'snp500_constituents.csv')
        snp.set_index('Symbol', inplace=True)
        px = pd.read_csv(DATA_FOLDER + 'prices.csv')
        px.Date = pd.to_datetime(px.Date)
        px.set_index('Date', inplace=True)
        px = px[px.columns[px.count() == px.count().max()]]

        snppx = pd.read_csv(DATA_FOLDER + 'snp500_HistData.csv')
        rets_monthly, cov_monthly = calcRetsCov(px, 'M')

        er_bmk = erk.annualize_rets(rets_monthly, 12)
        cov_bmk = rets_monthly.cov()

        return_bmk = (ew(er_bmk) * rets_monthly).sum(axis=1)
        wealth_bmk = erk.drawdown(return_bmk).Wealth

        return_port = {}
        wealth_port = {}

        return_port['bmk'] = return_bmk
        wealth_port['bmk'] = wealth_bmk

        for score in score_list:
                return_ = {}
                wealth_ = {}

                stock_selected = full_data.sort_values(score, ascending=False).head(num_of_stocks).index

                ## only need the expected return to generate the equal weights... other than that is redundant for now
                er_port = erk.annualize_rets(rets_monthly[stock_selected], 12)
                cov_port = rets_monthly[stock_selected].cov()

                return_ = (ew(er_port) * rets_monthly[stock_selected]).sum(axis=1)
                wealth_ = erk.drawdown(return_).Wealth

                return_port[score] = return_
                wealth_port[score] = wealth_

        return_port = pd.DataFrame(return_port)
        wealth_port = pd.DataFrame(wealth_port)
        
        
        df = pd.DataFrame()
        df['totalEsg'] = snppx.get("Open")
        df['bmk'] = snppx.get("Close")
        df['labels'] = snppx.get("Date")
        data = GraphData(df.get('labels'), df.get("bmk"), df.get("totalEsg"))
        
        response = make_response(jsons.dumps(data))
        response.headers['Access-Control-Allow-Origin'] = '*' 
        response.content_type = 'application/json'
        return response, 200
        
@app.get('/add_ticker')
def add_ticker():
        ticker = request.args.get('pticker')
        sticker = yfinq.Ticker(ticker)
        data_dict = sticker.asset_profile
        sector = data_dict[ticker]['industry']
        longName = data_dict[ticker]['longBusinessSummary']
        lArray = longName.split(',')
        #finalArray = [ticker, lArray[0], sector]
        field_names = ['Symbol', 'Name', 'Sector', 'CurrentPrice', 'ClosedPrice']
        dict = {"Symbol": ticker, "Name": lArray[0], "Sector": sector, 'CurrentPrice': 0, 'ClosedPrice': 0}

        with open (DATA_FOLDER + 'Portfolio.csv','a') as csv_file:
                dict_object = csv.DictWriter(csv_file, fieldnames=field_names)
                dict_object.writerow(dict)
                csv_file.close()
                
        response = make_response("Saved Successfully")
        response.headers['Access-Control-Allow-Origin'] = '*' 
        response.content_type = 'application/json'
        return response, 200
        
        if __name__ == '__main__':
            app.run('0.0.0.0', port=8099)
            app.debug = True
            