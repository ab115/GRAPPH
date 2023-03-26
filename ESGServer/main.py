import yahooquery as yfinq
import yfinance as yf
from flask import Flask, jsonify
import csv
from flask import request
import json
import pandas as pd
from tqdm.notebook import tqdm
import io
import sys
sys.path.append('./src')
from edge import *
from edge_risk_kit import *
import edge_risk_kit as erk

app = Flask(__name__)

@app.post('/save')
def save():
        if request.form.get('pticker')  != None and request.form.get('pticker')  != '':
                ticker = request.form.get('pticker')
                sticker = yfinq.Ticker(ticker)
                data_dict = sticker.asset_profile
                sector = data_dict[ticker]['industry']
                longName = data_dict[ticker]['longBusinessSummary']
                lArray = longName.split(',')
                cls = sticker.price[ticker]['regularMarketPreviousClose']
                curr = sticker.price[ticker]['regularMarketPrice']
                finalArray = [ticker, lArray[0], sector]
                field_names = ['Symbol', 'Name', 'Sector', 'CurrentPrice', 'ClosedPrice']
                dict = {"Symbol": ticker, "Name": lArray[0], "Sector": sector, "CurrentPrice": curr, "ClosedPrice": cls}
                with open ('Portfolio_workshop_draft.csv','a',newline='') as csv_file:
                        dict_object = csv.DictWriter(csv_file, fieldnames=field_names)
                        dict_object.writerow(dict)
                        csv_file.close()
        else:
                DATA_FOLDER = "./"
                with open(DATA_FOLDER + 'Portfolo_draft.txt') as file:
                        for item in file:
                                try:
                                        ticker = item.strip()
                                        sticker = yfinq.Ticker(ticker)
                                        #print(sticker)
                                        data_dict = sticker.asset_profile
                                        #print(data_dict)
                                        sector = data_dict[ticker]['industry']
                                        longName = data_dict[ticker]['longBusinessSummary']
                                        lArray = longName.split(',')
                                        cls = sticker.price[ticker]['regularMarketPreviousClose']
                                        curr = sticker.price[ticker]['regularMarketPrice']
                                        finalArray = [ticker, lArray[0], sector]
                                        field_names = ['Symbol', 'Name', 'Sector', 'CurrentPrice', 'ClosedPrice']
                                        dict = {"Symbol": ticker, "Name": lArray[0], "Sector": sector, "CurrentPrice": curr, "ClosedPrice": cls}
                                        with open('Portfolio_workshop_draft.csv', 'a', newline='') as csv_file:
                                                dict_object = csv.DictWriter(csv_file, fieldnames=field_names)
                                                dict_object.writerow(dict)
                                                csv_file.close()
                                except Exception as e:
                                        print(e)
                                        #print(".....")
                                        continue

        result = ""
        with open("Portfolio_workshop_draft.csv", "r+") as file:
                for line in file:
                        if not line.isspace():
                                result += line
                file.seek(0)
                file.write(result)

        return jsonify("Saved to file successfully"), 200

@app.get('/get_esg_scores')
def get_esg_scores():
        csvfile = open('esg_scores.csv', 'r')
        jsonfile = open('esg_scores.json', 'w')
        fieldnames = ("symbol", "socialScore", "governanceScore", "environmentScore", "totalEsg", "esgPerformance", "percentile", "peerGroup", "highestControversy")
        reader = csv.DictReader(csvfile, fieldnames)
        rowcount = 0
        responsedata = ''
        for row in reader:
                if rowcount == 0:
                        rowcount = rowcount + 1
                        #print(str(rowcount) + " " + str(row))
                        continue
                else:
                        json.dump(row, jsonfile, sort_keys=True, indent=4, separators=(',', ':'))
                        jsonfile.write(',')
                        jsonfile.write('\n')
                        #print(row)
                        responsedata = responsedata + str(row) + ','
        csvfile.close()
        jsonfile.close()
        responsedata = responsedata.rstrip(responsedata[-1])
        responsedata = "[{esg_scores:"+responsedata+"}]"
        #print(responsedata)
        return jsonify(responsedata), 200

@app.get('/get_snp_data')
def get_snp_data():
        DATA_FOLDER = "./"
        csvfile = open(DATA_FOLDER + 'snp500.csv', 'r')
        jsonfile = open('snp.json', 'w')
        fieldnames = ("Date", "Open", "High", "Low", "Close", "AdjClose", "Volume")
        reader = csv.DictReader(csvfile, fieldnames)
        rowcount = 0
        responsedata = ''
        for row in reader:
                if rowcount == 0:
                        rowcount = rowcount + 1
                        #print(str(rowcount) + " " + str(row))
                        continue
                else:
                        json.dump(row, jsonfile, sort_keys=True, indent=4, separators=(',', ':'))
                        jsonfile.write(',')
                        jsonfile.write('\n')
                        #print(row)
                        responsedata = responsedata + str(row) + ','
        csvfile.close()
        jsonfile.close()
        responsedata = responsedata.rstrip(responsedata[-1])
        responsedata = "[{snp500:"+responsedata+"}]"
        #print(responsedata)
        return jsonify(responsedata), 200

@app.route('/fetch_esg_scores', methods=['GET'])
def fetch_esg_scores():
        DATA_FOLDER = './'
        snp = pd.read_csv(DATA_FOLDER + 'Portfolio_workshop_draft.csv', encoding='unicode-escape')
        snp.set_index('Symbol', inplace=True)
        snp.head()
        esg_data = pd.DataFrame([])
        for ticker in tqdm(snp.index):
                try:
                        print('Processing {}'.format(ticker))
                        temp = yfinq.Ticker(ticker).esg_scores
                        tempdf = pd.DataFrame.from_dict(temp).T
                        tempdf['Symbol'] = str(ticker)
                        esg_data = pd.concat([esg_data, tempdf])
                except Exception as e:
                        print(e)
                        continue
        esg_data.set_index('Symbol', inplace=True)
        required_cols = ['socialScore', 'governanceScore', 'environmentScore', 'totalEsg',
                                 'esgPerformance', 'percentile', 'peerGroup', 'highestControversy']
        esg_data.columns.name = ''
        esg_data = esg_data[required_cols]
        esg_data = esg_data.apply(pd.to_numeric, errors='ignore')
        esg_data.shape
        esg_data.sort_values('totalEsg', ascending=False).head()
        esg_data.to_csv(DATA_FOLDER + 'esg_scores.csv')

        return jsonify("Success fetch esg scores"), 200

@app.route('/get_security_data', methods=['GET'])
def get_security_data():

        DATA_FOLDER = './'
        snp = pd.read_csv(DATA_FOLDER + 'Portfolio_workshop_draft.csv', encoding='unicode-escape')
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
        start_date = '2013-01-01'
        end_date = '2023-03-23'
        DATA_FOLDER = './'
        snp = pd.read_csv(DATA_FOLDER + 'Portfolio_workshop_draft.csv',encoding='unicode-escape')
        snp.set_index('Symbol', inplace=True)
        tickers = snp.index.to_list()
        data =yf.download(tickers, start=start_date, end=end_date)
        prices = data['Adj Close'][tickers]
        px = prices.loc['2013':].dropna(axis=1, how='all')
        px.to_csv(DATA_FOLDER + 'prices.csv')


        return jsonify("Success building prices"), 200

@app.route('/transpose_file', methods=['GET'])
def transpose_file():

        pd.read_csv('prices_to_be_transposed.csv', header=None).T.to_csv('prices_transposed', header=False, index=False)
        return jsonify("Success building prices"), 200



@app.route('/do_analytics', methods=['GET'])
def do_analytics():
        DATA_FOLDER = './'
        snp = pd.read_csv(DATA_FOLDER + 'Portfolio_workshop_draft.csv', encoding='unicode-escape')
        snp.set_index('Symbol', inplace=True)
        px = pd.read_csv(DATA_FOLDER + 'prices.csv')
        px.Date = pd.to_datetime(px.Date)
        px.set_index('Date', inplace=True)
        px = px[px.columns[px.count() == px.count().max()]]
        esg_data = pd.read_csv(DATA_FOLDER + 'esg_scores.csv')
        security_data = pd.read_csv(DATA_FOLDER + 'security_data.csv')
        esg_data.set_index('Symbol', inplace=True)
        security_data.set_index('symbol', inplace=True)
        rets_monthly, cov_monthly = calcRetsCov(px, 'M')
        rets_period = rets_monthly
        print(rets_monthly)
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
        full_data.head()
        full_data.shape
        #print(full_data.keys)
        #print(full_data)
        return full_data


def calcRetsCov(px, freq):
        px_freq = px.resample(freq).fillna('ffill')
        px_freq.index = px_freq.index.to_period(freq)
        rets = px_freq.pct_change().dropna(axis=1, how='all').dropna()
        cov = rets.cov()
        return rets, cov

@app.route('/do_panalysis', methods = ['GET'])
def do_panalysis():
        DATA_FOLDER = './'
        full_data = do_analytics()

        score_list = ['socialScore', 'governanceScore', 'environmentScore', 'totalEsg']
        #num_of_stocks = 30

        snp = pd.read_csv(DATA_FOLDER + 'Portfolio_workshop_draft.csv', encoding='unicode-escape')
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

                #stock_selected = full_data.sort_values(score, ascending=False).head(num_of_stocks).index
                stock_selected = full_data.sort_values(score, ascending=False).index
                ## only need the expected return to generate the equal weights... other than that is redundant for now
                er_port = erk.annualize_rets(rets_monthly[stock_selected], 12)
                cov_port = rets_monthly[stock_selected].cov()

                return_ = (ew(er_port) * rets_monthly[stock_selected]).sum(axis=1)
                wealth_ = erk.drawdown(return_).Wealth

                return_port[score] = return_
                wealth_port[score] = wealth_

        return_port = pd.DataFrame(return_port)
        wealth_port = pd.DataFrame(wealth_port)

        print(return_port)
        print(wealth_port)

        return jsonify("Success p_analysis"), 200

        if __name__ == '__main__':
            app.run('0.0.0.0', port=8099)