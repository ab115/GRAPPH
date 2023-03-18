import yahooquery as yfinq
from flask import Flask, jsonify
import csv
from flask import request
import json
import pandas as pd

app = Flask(__name__)

@app.post('/save_ticker')
def save_ticker():

        ticker = request.form.get('pticker')
        sticker = yfinq.Ticker(ticker)
        data_dict = sticker.asset_profile
        sector = data_dict[ticker]['industry']
        longName = data_dict[ticker]['longBusinessSummary']
        lArray = longName.split(',')
        finalArray = [ticker, lArray[0], sector]
        #print(finalArray)
        #print(ticker)
        #print(lArray[0])
        #print(sector)
        field_names = ['Symbol', 'Name', 'Sector']
        dict = {"Symbol": ticker, "Name": lArray[0], "Sector": sector}
        #print('dict', dict)

        with open ('snp_constituents_12.csv','a') as csv_file:
            dict_object = csv.DictWriter(csv_file, fieldnames=field_names)
            dict_object.writerow(dict)
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



        if __name__ == '__main__':
            app.run('0.0.0.0', port=8099)